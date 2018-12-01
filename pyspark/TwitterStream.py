import sys

import tweepy
import json
import socket
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener


class TweetsListener(StreamListener):
    def __init__(self, socket):
        print("Tweet listener initialized")
        self.client_socket = socket

    def on_data(self, data):
        try:
            jsonMessage = json.loads(data)
            if 'extended_tweet' in jsonMessage:
                message = jsonMessage['extended_tweet']['full_text']
            else:
                message = jsonMessage['text']
            print(message)
            self.client_socket.send(message.encode('utf-8'))
        except BaseException as e:
            print(str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def connect_to_twitter(connection):
    api_key = "VhqvkqzqZsyjsxpMw84IKnw6n"
    api_secret = "JHVawE5epZgROezuZtIw3JFKYalncCNX48oQaGB7d38A0xH2k0"
    access_token = "3820699198-SLcjMJAc9ASXA1BokLh2tk2idvRP9idFaA38nHr"
    access_token_secret = "2Qir9n2eBUaxKNe6aJQZsoOusFTZFLscxtzHUWx5lPEF3"

    auth = OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)

    twitter_stream = Stream(auth, TweetsListener(connection))
    twitter_stream.filter(track=["#"])


if __name__ == "__main__":
    s = socket.socket()
    host = "localhost"
    port = 7000
    s.bind((host, port))
    s.listen(5)

    connection, client_address = s.accept()

    connect_to_twitter(connection)
