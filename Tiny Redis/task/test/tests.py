import socket
import time
import threading

from hstest import dynamic_test, TestedProgram
from hstest.stage_test import StageTest
from hstest.check_result import CheckResult

def send_redis_command(host, port, command_parts):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1.0)
    
    try:
        sock.connect((host, port))
        command = format_redis_command(command_parts)
        sock.send(command.encode('utf-8'))
        
        try:
            response = sock.recv(1024)
            time.sleep(0.1)
            try:
                response += sock.recv(1024)
            except:
                pass
            return response
        except socket.timeout:
            if command_parts and command_parts[0].upper() == 'EXIT':
                return 'OK'
            raise
        
    finally:
        sock.close()

def create_persistent_connection(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(3.0)
    sock.connect((host, port))
    return sock

def send_command_persistent(sock, command_parts):
    command = format_redis_command(command_parts)
    sock.send(command.encode('utf-8'))

def receive_message(sock):
    try:
        response = sock.recv(1024)
        return response
    except socket.timeout:
        return None

def format_redis_command(parts):
    command = f"*{len(parts)}\r\n"
    for part in parts:
        part_str = str(part)
        command += f"${len(part_str)}\r\n{part_str}\r\n"
    return command

def subscribe_persistent(channel_name, sock):
    return send_command_persistent(sock, ['SUBSCRIBE', channel_name])

def publish_persistent(channel_name, message, sock):
    return send_command_persistent(sock, ['PUBLISH', channel_name, message])

def exit_client(host='127.0.0.1', port=6379):
    return send_redis_command(host, port, ['EXIT'])


class TinyRedisTest(StageTest):
    @dynamic_test
    def test_pubsub(self):
        main = TestedProgram(self.source_name)
        main.start_in_background()
        time.sleep(0.2)
        
        try:
            subscriber_1 = create_persistent_connection('127.0.0.1', 6379)
            subscriber_2 = create_persistent_connection('127.0.0.1', 6379)
            publisher = create_persistent_connection('127.0.0.1', 6379)
        
            subscribe_persistent('sports', subscriber_1)
            subscribe_persistent('sports', subscriber_2)
            # Try to fetch first responses if user sends something on subscription.
            receive_message(subscriber_1)
            receive_message(subscriber_2)

            publish_persistent('sports', 'GOAL!', publisher)
            
            message_response_1 = receive_message(subscriber_1)
            message_response_2 = receive_message(subscriber_2)
            
            if all([resp and b'GOAL!' in resp for resp in (message_response_1, message_response_2)]):
                result = CheckResult.correct()
            else:
                result = CheckResult.wrong(f'Expected to receive "GOAL!" for both subscribers, got {message_response_1} for subscriber 1 & {message_response_2} for subscriber 2')
            
        except Exception as e:
            result = CheckResult.wrong(f'Got a connection error: {e}')
        
        finally:
            try:
                subscriber_1.close()
                subscriber_2.close()
                publisher.close()
                exit_client()
            except:
                pass
        
        return result

    @dynamic_test
    def test_subscriber_disconnects(self):
        # Wait for previous test to finish
        time.sleep(3)
        main = TestedProgram(self.source_name)
        main.start_in_background()
        time.sleep(0.2)
        
        try:
            subscriber = create_persistent_connection('127.0.0.1', 6379)
            publisher = create_persistent_connection('127.0.0.1', 6379)
            subscribe_persistent('sports', subscriber)
            
            subscriber.close()
            time.sleep(0.1)
            
            publish_persistent('sports', 'GOAL!', publisher)
            
            result = CheckResult.correct()
            
        except Exception as e:
            result = CheckResult.wrong(f'Server crashed or failed with an error: {e}')
        
        finally:
            try:
                publisher.close()
                exit_client()
            except:
                pass
        
        return result