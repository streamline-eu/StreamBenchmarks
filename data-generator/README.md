#  Simple data generator

This is (simplified) data generator that initially will be used in our project. I removed some features from the code as for now we need simple, plain generator.
The processing engine connects with data generator via socket. 
Generator consists of two main parts (threads). Producer thread produces/pushes records to queue in a given speed. Consumer thread serves the data processing system by reading tuples from queue and sending. The queue is protected by locks to avoid possible race condiditions between threads.  

The generated records are transmitted in byte array (unsigned char array) format:
Each record has 3 fields: `key`, `value` and `ts`. `key` and `value` have 4 byte `int` type, `ts` has 8 byte `unsigned long` type. 


`key` and `value` fields are arbitrart keys and values for record. Keys are evenly distributed. 
`ts` is the event timestamp of record. Record is generated with `ts` and then pushed to FIFO queue. So the longer it stays in the queue, the more latency it has. 

## Quickstart

- Compile the code with `gcc main.c -o myLovelyExecutableBinary -lpthread`
- run with `./myLovelyExecutableBinary commandlineArguments`. The `commandlineArguments`  are: 
    * `--count`: the number input tuples to be generated. This is required argument.
    * `--logInterval`: the interval of between which the generator prints producer and consumer throughputs. For example `--logInterval 10` means after every 10 elements generated, the generator will print the current producer and consumer throughput to stdout. This is an optional argument, default is `1000000`
    * `--port`: the port that data will be read by data processing system. This is required argument.
    * `--sleepTime` and  `--dataGeneratedAfterEachSleep`: if user wants to test the underlying data processing system with different throughputs, with this arguments one can adjust the desired throughput. For example   `--sleepTime 1000 --dataGeneratedAfterEachSleep 1` means every 1 second (1000 ms) 1 tuple will be generated. Both are required arguments.
    * `--spikeInterval` and `--spikeMagnitute`: used when user wants to simulate spikes. For example, `--spikeInterval 1000 --spikeMagnitute 2` means generator will increase and decrease the data generation speed by orders of `2` in every generated `1000` generated tuples. Both are optional arguments. By default, we don't have spikes. 

An example setup can be:

`./myLovelyExecutableBinary --dataGeneratedAfterEachSleep 1 --count 900000 --port 9291 --logInterval 1000000 --sleepTime 1000` meaning, generate total `900000` tuples, `1` tuple per `1` millisecond, report throughput metric to user for every `1000000` tuples generated and listen to streaming system on port `9291`.

To test it, you can fire `sample_client.cpp`. It will listen `127.0.0.1:9291` deserialize tuples and print `key`, `value` and `ts` values to stdout. Alternatively, you can use `telnet`.
