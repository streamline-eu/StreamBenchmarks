
#define _POSIX_C_SOURCE 200809L

#include<stdio.h>
#include<string.h>    //strlen
#include<sys/socket.h>
#include<arpa/inet.h> //inet_addr
#include<unistd.h>    //write
#include <stdlib.h>
#include<math.h>
#include <pthread.h>
#include <sys/time.h>
#include<semaphore.h>
#include <getopt.h>

const char *geoListAll[] = {
        "AF","AX" ,"AL","DZ","AS","AD","AO","AI","AQ","AG","AR","AM","AW","AC","AU","AT","AZ","BS","BH","BB",
        "BD","BY","BE","BZ", "BJ","BM","BT","BW","BO","BA","BV","BR","IO","BN","BG","BF","BI","KH","CM","CA",
        "CV","KY","CF","TD","CL","CN","CX","CC","CO","KM","CG","CD","CK","CR","CI","HR", "CU","CY","CZ","CS",
        "DK","DJ","DM","DO","TP","EC","EG","SV","GQ","ER","EE","ET","EU","FK","FO","FJ","FI","FR","FX","GF",
        "PF","TF","MK","GA","GM","GE","DE","GH","GI","GB","GR","GL","GD","GP","GU","GT","GG","GN","GW","GY",
        "HT","HM","HN","HK","HU","IS","IN","ID","IR","IQ","IE","IL","IM","IT","JE","JM","JP","JO","KZ","KE",
        "KI","KP","KR","KW","KG","LA","LV","LB","LI","LR","LY","LS","LT","LU","MO","MG","MW","MY","MV","ML",
        "MT","MH","MQ","MR","MU","YT","MX","FM","MC","MD","MN","ME","MS","MA","MZ","MM","NA","NR","NP","NL"


};
unsigned long benchmarkCount;
unsigned long geoIndex=0;
int maxPrice = 1000;
char ** buffer;
int port;
unsigned long logInterval;
sem_t sem;
unsigned long sleepTime;
int socket_desc , client_sock , c , read_size;
struct sockaddr_in server , client;

typedef struct LogInfo {
    unsigned long long key;
    unsigned long value;
    unsigned long throughput;
}  logInfo;

logInfo**  producerLog;
logInfo** consumerLog;
int dataGeneratedAfterEachSleep;
int sustainability_limit;
int spikeInterval;
int spikeMagnitute;
int isSpikeActive = 0;
int allSize = sizeof(geoListAll)/sizeof(geoListAll[0]);


unsigned long long  get_current_time_with_ms (void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    unsigned long long millisecondsSinceEpoch = (unsigned long long)(tv.tv_sec) * 1000 + (unsigned long long)(tv.tv_usec) / 1000;
    return  millisecondsSinceEpoch;
}

int msleep(unsigned long milisec)
{
    struct timespec req={0};
    time_t sec=(int)(milisec/1000);
    milisec=milisec-(sec*1000);
    req.tv_sec=sec;
    req.tv_nsec=milisec*1000000L;
    while(nanosleep(&req,&req)==-1)
        continue;
    return 1;
}


char* generateJsonString( unsigned long currentIndex){
    char * newJson = malloc(100);
    sprintf(newJson,"{\"key\":\"%s\",\"value\":\"%d\",\"ts\":\"%llu\"}\n \0", geoListAll[geoIndex] , rand() % maxPrice,get_current_time_with_ms());

    geoIndex++;
    geoIndex = geoIndex % allSize;

    return newJson;
}

void *produce( void  )
{
    int logIndex = 0;
    producerLog = malloc(((benchmarkCount/logInterval) +1) * sizeof (*producerLog));
    producerLog[logIndex] = malloc(sizeof(logInfo));
    producerLog[logIndex]->value = 0;
    producerLog[logIndex]->key = get_current_time_with_ms()/1000;
    producerLog[logIndex]->throughput = 0;
    unsigned long long startTime =     producerLog[logIndex]->key;

    int queue_size ;
    for (unsigned long i = 0; i < benchmarkCount; ){
        for(int k = 0; k < dataGeneratedAfterEachSleep && i < benchmarkCount; k++ ,i++){

            buffer[i] = generateJsonString(i);

            if(i % logInterval == 0){
                sem_getvalue(&sem, &queue_size);

                unsigned long long sec  = get_current_time_with_ms()/1000;
                if (producerLog[logIndex]->key != get_current_time_with_ms()/1000){
                    logIndex++;
                }
                producerLog[logIndex] = malloc(sizeof(logInfo));
                producerLog[logIndex]->value = i;
                producerLog[logIndex]->key = sec;
                producerLog[logIndex]->throughput = 0;
                unsigned long long interval =   producerLog[logIndex]->key - startTime;
                if(interval != 0){
                    producerLog[logIndex]->throughput = i /  interval;
                }
                printf("Producer info - Current record timestamp - %llu, Current record index - %lu, Throughput - %lu \n", producerLog[logIndex]->key, producerLog[logIndex]->value, producerLog[logIndex]->throughput );
            }
            sem_post(&sem);

            if (spikeInterval !=0 && spikeMagnitute !=0 && i % spikeInterval == 0 ) {
                if (isSpikeActive == 0) {
                    isSpikeActive = 1;
                    dataGeneratedAfterEachSleep = dataGeneratedAfterEachSleep * spikeMagnitute;
                    printf("Onn %d \n", dataGeneratedAfterEachSleep);
                } else {
                    isSpikeActive = 0;
                    dataGeneratedAfterEachSleep = dataGeneratedAfterEachSleep / spikeMagnitute;
                    printf("Off %d \n", dataGeneratedAfterEachSleep);
                }
            }
        }


        if(sleepTime != 0){
            msleep(sleepTime );
        }
    }
    logIndex++;
    producerLog[logIndex] = NULL;

}



void *consume( void  )
{
    if (client_sock < 0)
    {
        perror("accept failed");
        return (void*)1;
    }
    puts("Connection accepted");


    // sending tuples
    int logIndex = 0;

    consumerLog = malloc(((benchmarkCount/logInterval) +3) * sizeof (*consumerLog));
    consumerLog[logIndex] = malloc(sizeof(logInfo));
    consumerLog[logIndex]->value = 0;
    consumerLog[logIndex]->key = get_current_time_with_ms()/1000;
    consumerLog[logIndex]->throughput = 0;
    unsigned long long startTime = consumerLog[logIndex]->key;

    for (unsigned long i = 0; i < benchmarkCount; i ++){
        sem_wait(&sem);
        write(client_sock , buffer[i] , 100);
        if(i % logInterval == 0){

            unsigned long long sec  = get_current_time_with_ms()/1000;
            if (consumerLog[logIndex]->key != get_current_time_with_ms()/1000){
                logIndex++;
            }
            consumerLog[logIndex] = malloc(sizeof(logInfo));
            consumerLog[logIndex]->value = i;
            consumerLog[logIndex]->key = sec;
            consumerLog[logIndex]->throughput = 0;
            unsigned long long interval = consumerLog[logIndex]->key - startTime;
            if(interval != 0){
                consumerLog[logIndex]->throughput = i / interval;
            }
            printf("Consumer log - Current record timestamp - %llu, Current record index - %lu, Throughput - %lu \n", consumerLog[logIndex]->key,consumerLog[logIndex]->value,consumerLog[logIndex]->throughput );
        }
        free(buffer[i]);
    }
    logIndex++;
    consumerLog[logIndex]=NULL;


    printf("All data read by system \n");
    msleep(1000 * 1000);

    if(read_size == 0)
    {
        puts("Client disconnected");
    }
    else if(read_size == -1)
    {
        perror("recv failed");
    }
}

void fireServerSocket(void){
    socket_desc = socket(AF_INET , SOCK_STREAM , 0);
    if (socket_desc == -1)
    {
        printf("Could not create socket");
    }
    puts("Socket created");
    //Prepare the sockaddr_in structure
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons( port );
    //Bind
    if( bind(socket_desc,(struct sockaddr *)&server , sizeof(server)) < 0)
    {
        //print the error message
        perror("bind failed. Error");
        return ;
    }
    puts("bind done");
    //Listen
    listen(socket_desc , 3);
    //Accept and incoming connection
    puts("Waiting for incoming connections...");
    c = sizeof(struct sockaddr_in);

    //accept connection from an incoming client
    client_sock = accept(socket_desc, (struct sockaddr *)&client, (socklen_t*)&c);
}



void print_usage() {
    printf("Usage: myLovelyExecutableBinary --count num [--logInterval num] --port num --sleepTime num --dataGeneratedAfterEachSleep num [--sustainabilityLimit num] [--spikeInterval num] [--spikeMagnitute num]\n");
}

void parse_arguments(int argc, char * argv[]) {
    static struct option long_options[] = {
            {"count",       required_argument,0,  'c' },
            {"logInterval", required_argument, 0,  'l' },
            {"port",        required_argument, 0,  'p' },
            {"sleepTime",   required_argument, 0,  's' },
            {"dataGeneratedAfterEachSleep", required_argument, 0,  'g' },
            {"sustainabilityLimit", required_argument, 0,  'u' },
            {"spikeInterval",   required_argument, 0,  'i' },
            {"spikeMagnitute",   required_argument, 0,  'm' },
            {0,           0,                 0,  0   }
    };
    int opt = 0;
    int long_index =0;

    benchmarkCount, port, sleepTime, dataGeneratedAfterEachSleep = -1;

    // default values for variables
    logInterval = 1000000;
    sustainability_limit = 10000;
    spikeInterval = 0;
    spikeMagnitute = 0;

    while ((opt = getopt_long(argc, argv,"apl:b:",
                              long_options, &long_index )) != -1) {
        switch (opt) {
            case 'c' : benchmarkCount = atoi(optarg);
                break;
            case 'l' : logInterval = atoi(optarg);
                break;
            case 'p' : port = atoi(optarg);
                break;
            case 's' : sleepTime = atoi(optarg);
                break;
            case 'g' : dataGeneratedAfterEachSleep = atoi(optarg);
                break;
            case 'u' : sustainability_limit = atoi(optarg);
                break;
            case 'i' : spikeInterval = atoi(optarg);
                break;
            case 'm' : spikeMagnitute = atoi(optarg);
                break;
            default: print_usage();
                exit(EXIT_FAILURE);
        }
    }

    if (benchmarkCount == -1 || port ==-1 || sleepTime ==-1 || dataGeneratedAfterEachSleep ==-1) {
        print_usage();
        exit(EXIT_FAILURE);
    }


}


int main(int argc , char *argv[])
{
    parse_arguments(argc, argv);
    pthread_t producer, consumer;
    int seed = 123;
    srand(seed);
    sem_init(&sem, 0 , 0);
    buffer = malloc (benchmarkCount * sizeof(*buffer));
    fireServerSocket();
    //initLogFiles();
    pthread_create( &producer, NULL, produce, NULL);
    pthread_create( &consumer, NULL, consume, NULL);

    pthread_join( producer, NULL);
    pthread_join( consumer, NULL);
    free(buffer);
    return 0;
    //Send ehe message back to client
}