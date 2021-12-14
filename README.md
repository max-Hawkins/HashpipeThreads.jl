# HashpipeThreads.jl - Hashpipe threads in Julia!

## Overview

This repository is a collection of [Hashpipe](https://github.com/david-macmahon/hashpipe) threads written in the Julia programming language. They utilize [Hashpipe.jl](https://github.com/max-Hawkins/Hashpipe.jl), a Julian interface to the C Hashpipe code, for operation with minimal overhead compared to C-native Hashpipe threads. These threads are compatible with C Hashpipe threads but can also be used in a purely-Julian data processing pipeline. Each of these threads includes a few items:

- A thread-specific struct that's a subtype of the abstract HashpipeThread. This contains all the metadata necessary for the thread to operate.

- An initialization function that executes any and all one-time startup tasks like preallocating memory for data, opening files, and verification of configuration parameters.

- An execution function that runs a single compute iteration of the Hashpipe thread.

See [the template thread](hp_thread_template.jl) for help creating your own thread.

## Running threads

Since these Julian threads can be operated in coordination with C Hashpipe threads, these threads will either be operated in a purely Julian Hashpipe pipeline or in combination with C Hashpipe threads. In C, Hashpipe initializes each thread from the end of the pipeline forwards and then starts each thread running from the start to the end. However, since each Julian thread is its own process, this coordination is difficult, so a Julain thread is initialized and then immediately started.