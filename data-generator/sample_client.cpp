#include <stdio.h>
#include <string.h>    //strlen
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <cstdint>


int deserialize_uint32(unsigned char *buffer, int index)
{
    int value = 0;

    value |= buffer[index] << 24;
    value |= buffer[index + 1] << 16;
    value |= buffer[index + 2] << 8;
    value |= buffer[index + 3];
    return value;
}

 unsigned  long deserialize_uint64(unsigned char *buffer, int index)
{
   unsigned  long value = 0;
    for (int j = 0; j < 8; j++) {
        value = (value << 8) | buffer[index + j];
    }
    return value;
}


int main() {
    int sock;
    struct sockaddr_in server;
    unsigned char buffer[16];

    //Create socket
    sock = socket(AF_INET , SOCK_STREAM , 0);
    if (sock == -1)
    {
        std::cout << "Could not create socket" << std::endl;
        return -1;
    }

    //ip address of localhost
    server.sin_addr.s_addr = inet_addr("127.0.0.1");
    server.sin_family = AF_INET;
    server.sin_port = htons( 9291 );

    if (connect(sock,(struct sockaddr *) &server,sizeof(struct sockaddr_in)) < 0)
        std::cerr << "ERROR connecting" << std::endl;


    int received_bytes = 0;
    while( received_bytes=recv(sock, buffer, 16, 0) > 0){
        std::cout << "key is " << deserialize_uint32(buffer, 0) << std::endl;
        std::cout << "value is " << deserialize_uint32(buffer, 4) << std::endl;
        std::cout << "ts is " << deserialize_uint64(buffer, 8) << std::endl;
    }

    free(buffer);
    close(sock);
    std::cout << "Finished" << std::endl;
    return 0;
}