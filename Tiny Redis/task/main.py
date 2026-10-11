import socket
import threading
import time


class RESPProtocolError(Exception):
    pass


class TinyRedis:
    """
    Tiny Redis is a simple in-memory key-value store that supports basic Redis commands.
    It provides a basic implementation of Redis commands such as SET, GET, DEL, and PUBLISH.
    The data is stored in memory and has a default timeout of 1 hour.
    """
    DELIMITER = b'\r\n'
    STORAGE_TIMEOUT = 3600
    BUFFER_SIZE = 2048

    def __init__(self, hostname: str = "0.0.0.0", port: int = 6379) -> None:
        self.hostname = hostname
        self.port = port

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind((self.hostname, self.port))
        self.server.listen(10)
        self.server.settimeout(0.5)
        self.is_running = False

        self.data_store = {}
        self.subscribers = {}

        self.client_threads = []
        self.thread_lock = threading.Lock()
        self.subscribers_lock = threading.Lock()

    def start(self) -> None:
        """
        Starts the server, accepting and handling client connections in separate threads.

        This method runs an event loop to manage incoming client connections. Each client
        connection is handled in a separate thread. The server continues to accept
        connections and process clients until the `self.is_running` flag is set to `False`.
        After stopping, this method ensures all client threads are joined and performs
        necessary cleanup operations.

        :raises OSError: If there are issues with socket operations while accepting
            client connections.
        """

        self.is_running = True
        while self.is_running:
            try:
                client_socket, client_address = self.server.accept()
            except socket.timeout:
                continue

            client_thread = threading.Thread(
                target=self.handle_client, args=(client_socket,), daemon=True
            )
            client_thread.start()
            self.client_threads.append(client_thread)

        for client_thread in self.client_threads:
            client_thread.join()
        self.close()

    def handle_client(self, client_socket: socket.socket) -> None:
        """
        Handles communication with a connected client socket, processing received data and sending appropriate responses.

        This method continuously listens for incoming data from a connected client socket while the server is running.
        It processes received commands and sends responses back to the client if applicable. If the connection is closed
        or no data is received, the socket is closed.

        :param client_socket: The connected client's socket object used for communication.
        :type client_socket: socket.socket
        :return: None
        """
        try:
            while self.is_running:
                try:
                    content = client_socket.recv(self.BUFFER_SIZE)
                except OSError:
                    break

                if not content:
                    break

                response = self.handle_command(content, client_socket)
                if response:
                    try:
                        client_socket.sendall(response)
                    except OSError:
                        break
        finally:
            with self.subscribers_lock:
                self.remove_subscriber(client_socket)
            client_socket.close()

    def handle_command(self, message: bytes, client_socket: socket.socket) -> bytes:
        """
        Handles a command received from a client, parses it, and invokes the appropriate
        command-specific method based on the message type. Supports various predefined
        commands such as `GET`, `SET`, `SUBSCRIBE`, `PUBLISH`, `DEL`, `PING`, `EXIT`,
        and `ECHO`. If the command type is unrecognized, it returns a response for an
        unknown command. Any command parsing errors result in an appropriate error
        response.

        :param message: The raw command message received from the client, expected in
            RESP protocol format.
        :type message: bytes
        :param client_socket: The socket object representing the client connection.
        :return: A response in bytes that will be sent to the client as per the command's
            outcome.
        """
        try:
            parsed_message = self.parse_command(message)
        except RESPProtocolError:
            return self.parsing_error_response()

        if not parsed_message:
            return self.parsing_error_response()

        command = parsed_message[0].upper()
        arguments = parsed_message[1:]

        match command:
            case b"GET":
                return self.get_command(arguments)
            case b"SET":
                return self.set_command(arguments)
            case b"SUBSCRIBE":
                return self.subscribe_command(arguments, client_socket)
            case b"PUBLISH":
                return self.publish_command(arguments)
            case b"DEL":
                return self.del_command(arguments)
            case b"PING":
                return self.ping_command(arguments)
            case b"EXIT":
                return self.exit_command()
            case b"ECHO":
                return self.echo_command(arguments)
            case _:
                return self.unknown_command()

    def close(self) -> None:
        """
        Closes the server connection.

        This method is used to properly close the server connection and release
        any related system resources. It ensures that the server is no longer
        accessible or listening for incoming connections.

        :return: None
        """
        self.server.close()

    def get_command(self, arguments: list[bytes]) -> bytes:
        """
        Fetches a command based on the given arguments from the data store.

        This method retrieves data corresponding to the given arguments from the
        data store. If the data has an associated expiration time and it has expired,
        the data is deleted from the store. The retrieved data is then encoded into
        a response message.

        :param arguments: A list where the first element should be a byte-encoded key that
            is used to look up data in the data store.
        :return: A byte-encoded response message containing the retrieved data or None
            if the data was not found or has expired.
        """
        if len(arguments) < 1:
            return self.parsing_error_response()

        with self.thread_lock:
            data_key = arguments[0].decode()
            data, expiration_time = self.data_store.get(data_key, (None, None))
            if expiration_time and expiration_time < time.monotonic():
                del self.data_store[data_key]
                data = None
        return self.encode_message([data])

    def set_command(self, arguments: list[bytes]) -> bytes:
        """
        Constructs and processes a "SET" command, storing the given key-value pair in the data store and
        optionally setting an expiration time for the key.

        :param arguments: List containing the arguments for the "SET" command. The first element is the key
            as bytes, the second element is the value as bytes, and optionally, the third and fourth
            elements are the "EX" flag as bytes and the expiration time as bytes.
        :return: A "bytes" response indicating the success or failure of the operation.
        """
        if len(arguments) not in (2, 4):
            return self.parsing_error_response()

        with self.thread_lock:
            data_key = arguments[0].decode()
            data_value = arguments[1]
            if len(arguments) == 4:

                if arguments[2].upper() != b"EX":
                    return self.parsing_error_response()
                try:
                    expiration_time = int(arguments[3].decode())
                except ValueError:
                    return self.parsing_error_response()

                if expiration_time < 0:
                    return self.parsing_error_response()

                expiration_timestamp = expiration_time + time.monotonic()

            else:
                expiration_timestamp = None
            self.data_store[data_key] = (data_value, expiration_timestamp)
        return self.ok_response()

    def subscribe_command(self, arguments: list[bytes], client_socket: socket.socket) -> bytes:
        """
        Handles the SUBSCRIBE command by adding the given client socket to the
        specified channel(s) subscription list. If a channel does not exist, it
        will be created.

        :param arguments: A list of channel names to which the client wishes to subscribe.
        :param client_socket: The socket of the client subscribing to the channels.
        :type arguments: list[bytes]
        :return: A success response as a byte string if the command is processed correctly, or an
            error response as a byte string if the arguments are invalid.
        :rtype: bytes
        """
        if len(arguments) < 1:
            return self.parsing_error_response()

        with self.subscribers_lock:
            for channel in arguments:
                channel_name = channel.decode()
                if channel_name not in self.subscribers:
                    self.subscribers[channel_name] = set()
                self.subscribers[channel_name].add(client_socket)

        return self.ok_response()

    def publish_command(self, arguments: list[bytes]) -> bytes:
        """
        Publishes a message to a specified channel and sends it to all subscribers of that channel.

        If a subscriber fails to receive the message due to an error (e.g., disconnection), it
        will be removed from the channel's subscription list.

        :param arguments: A list where the first element is the channel name as a byte string
            and the second element is the message to publish as a byte string.
        :return: The appropriate response as a byte string.
        """
        if len(arguments) != 2:
            return self.parsing_error_response()

        channel = arguments[0].decode()
        message = arguments[1]

        with self.subscribers_lock:
            subscribers = list(self.subscribers.get(channel, set()))

            disconnected_subscribers = []

            for subscriber in subscribers:
                try:
                    subscriber.sendall(self.encode_message([message]))
                except OSError:
                    disconnected_subscribers.append(subscriber)

            for subscriber in disconnected_subscribers:
                self.remove_subscriber(subscriber)

            return self.ok_response()

    def remove_subscriber(self, client_socket: socket.socket) -> None:
        """
        Removes a subscriber from all subscribed channels and cleans up any channels
        that no longer have subscribers.

        :param client_socket: The socket representing the subscriber to be removed.
        :type client_socket: socket.socket
        :return: None
        """
        for channel in list(self.subscribers.keys()):
            self.subscribers[channel].discard(client_socket)

            if not self.subscribers[channel]:
                del self.subscribers[channel]

    def del_command(self, arguments: list[bytes]) -> bytes:
        """
        Deletes an entry from the data store corresponding to the given key. If the key exists in the
        data store, it is removed, and an OK response is generated.

        :param arguments: A list where the first element is a byte-encoded key to delete from the
            data store.
        :type arguments: list[bytes]
        :return: A byte-encoded response indicating the success of the delete operation.
        :rtype: bytes
        """
        num_of_del_keys = 0
        with self.thread_lock:
            for data_key in arguments:
                key = data_key.decode()
                if key in self.data_store:
                    del self.data_store[key]
                    num_of_del_keys += 1

        num_of_del_keys = str(num_of_del_keys).encode()
        return self.encode_message([num_of_del_keys])

    def ping_command(self, args: list[bytes]) -> bytes:
        """
        Executes the PING command, which responds with a 'PONG' message when
        called correctly. If the command includes more than the required
        arguments, an error response is returned.

        :param args: A list of command arguments provided by the client.
        :type args: list
        :return: A byte-encoded PONG response if the argument count is valid,
                 otherwise an error response in bytes.
        :rtype: bytes
        """
        if len(args) > 1:
            return self.parsing_error_response()
        return b"+PONG\r\n"

    def exit_command(self) -> bytes:
        """
        Marks the exit command for the application by setting the internal state to indicate
        the application should stop running. Returns a confirmation message as bytes.

        :return: A byte response indicating the exit command has been executed.
        :rtype: bytes
        """
        self.is_running = False
        return b"+Bye\r\n"

    def echo_command(self, args: list[bytes]) -> bytes:
        """
        Processes the ECHO command by validating and returning its argument.

        The method expects a single argument and will return an encoded
        message containing the argument if it is valid. If the number of
        arguments is incorrect, a parsing error response is generated.

        :param args: A list of byte strings representing the command arguments.
        :type args: list[bytes]
        :return: An encoded byte message containing the argument if valid, or
                 a parsing error response otherwise.
        :rtype: bytes
        """
        if len(args) != 1:
            return self.parsing_error_response()

        value = args[0]
        return self.encode_message([value])

    def unknown_command(self) -> bytes:
        """
        Provides a response for unrecognized commands.

        :return: A predefined response in bytes indicating that the command is 
                 unrecognized.
        :rtype: bytes
        """
        return b"-Unknown command!\r\n"

    def encode_message(self, args: list[bytes | None]) -> bytes:
        """
        Encodes a list of byte strings into a unified message format. Each byte string 
        in the input is formatted with its length and content, separated by specific 
        control characters. If any element in the input list is `None`, returns a 
        predefined byte string indicating a null response.

        :param args: A list of byte strings (`list[bytes]`) to be encoded. If an 
            element in the list is `None`, it implies a null input.
        :return: A single byte string (`bytes`) containing the encoded message. For null 
            inputs, a predefined null response `$-1\r\n` is returned.
        """
        if None in args:
            return b"$-1\r\n"

        message = f""
        for arg in args:
            message += f"${len(arg)}\r\n{arg.decode()}\r\n"

        return message.encode()

    def parse_command(self, message: bytes) -> list[bytes]:
        """
        Parses a Redis protocol message and extracts its components.

        This method takes a message in the Redis protocol format and converts it into
        its constituent components. It utilizes the helper method `parse_redis_protocol` 
        to perform the extraction.

        :param message: The raw Redis protocol message to parse.
        :type message: bytes
        :return: The components of the message as parsed from the Redis protocol.
        :rtype: list
        """
        message_components = self.parse_redis_protocol(message)
        return message_components

    def read_line(self, message: bytes, pos: int) -> tuple[bytes, int]:
        """
        Reads a single line from the provided message starting from a specific position. The method attempts 
        to locate the delimiter in the message starting from the specified position. If the delimiter is not 
        found, it raises a protocol-specific error indicating a malformed input. On success, it extracts and 
        returns the line content up to the delimiter and updates the position to the index following the 
        delimiter.

        :param message: The complete message as a string to read lines from.
        :type message: str
        :param pos: The starting position within the `message` to begin searching for the delimiter.
        :type pos: int
        :return: A tuple where the first value is the extracted line without the delimiter, and the second 
                 value is the updated position after the delimiter.
        :rtype: tuple[str, int]
        :raises RESPProtocolError: If the delimiter is missing in the expected portion of the message.
        """
        end = message.find(self.DELIMITER, pos)
        if end == -1:
            raise RESPProtocolError("Missing CRLF terminator")

        line = message[pos:end]
        pos = end + len(self.DELIMITER)
        return line, pos

    def read_exact(self, message: bytes, count: int, pos: int) -> tuple[bytes, int]:
        """
        Reads an exact number of characters from a given message starting at the specified position.

        The method extracts a specific segment from the message by reading the exact number of 
        characters specified by the `count` parameter. If the requested range exceeds the length
        of the message, an error is raised to signify an unexpected termination.

        :param message: The input message from which data is extracted.
        :type message: bytes
        :param count: The exact number of characters to read from the message.
        :type count: int
        :param pos: The starting position in the message from which reading begins.
        :type pos: int
        :return: A tuple containing the extracted data and the updated position after reading.
        :rtype: tuple[bytes, int]
        :raises RESPProtocolError: If the requested range exceeds the length of the message.
        """
        if pos + count > len(message):
            raise RESPProtocolError("Unexpected end of message")

        data = message[pos:pos + count]
        pos += count
        return data, pos

    # TODO: FIX exceptions
    def parse_redis_protocol(self, message: bytes) -> list[bytes]:
        """
        Parses a message following the RESP (REdis Serialization Protocol) protocol and 
        extracts the array of bulk strings. The input `message` is expected to comply 
        with the RESP format, including appropriate headers and terminators for each 
        element.

        :param message: The byte-encoded message adhering to the RESP protocol.
        :type message: bytes
        :return: A list of byte-encoded bulk string elements parsed from the RESP message.
        :rtype: list[bytes]
        :raises RESPProtocolError: If the `message` does not comply with the RESP protocol 
            specifications, such as invalid headers, lengths, or missing terminators.
        """
        pos = 0

        header, pos = self.read_line(message, pos)

        if not header.startswith(b"*"):
            raise RESPProtocolError("Invalid RESP protocol header")

        try:
            num_elements = int(header[1:])
        except ValueError:
            raise RESPProtocolError(f"Invalid array length: {header!r}")

        if num_elements < 0:
            raise RESPProtocolError("Negative array length")

        elements = []
        for _ in range(num_elements):
            bulk_header, pos = self.read_line(message, pos)
            if not bulk_header.startswith(b"$"):
                raise RESPProtocolError(f"Expected bulk string header, got: {bulk_header!r}")

            try:
                length = int(bulk_header[1:])
            except ValueError:
                raise RESPProtocolError(f"Invalid bulk length: {bulk_header!r}")

            if length < 0:
                raise RESPProtocolError("Negative bulk length")

            payload, pos = self.read_exact(message, length, pos)

            terminator, pos = self.read_exact(message, len(self.DELIMITER), pos)
            if terminator != self.DELIMITER:
                raise RESPProtocolError(f"Expected CRLF after bulk string, got: {terminator!r}")

            elements.append(payload)

        if pos != len(message):
            raise RESPProtocolError("Trailing data after command")

        return elements

    def parsing_error_response(self) -> bytes:
        """
        Provides a response message indicating a parsing error.

        :return: A byte string containing the parsing error response.
        :rtype: bytes
        """
        return b"-Parsing error!\r\n"

    def ok_response(self) -> bytes:
        """
        Provides a predefined positive response for use in communication protocols.

        :return: A byte string containing the response message "+OK\r\n"
        :rtype: bytes
        """
        return b"+OK\r\n"


if __name__ == "__main__":
    redis = TinyRedis()
    redis.start()
