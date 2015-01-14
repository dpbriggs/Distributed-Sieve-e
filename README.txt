Distributed Sieve of Eratosthenes

David Briggs

Source files: src/mail-sieve-e/*
Github Page:  https://github.com/iMultiPlay/Distributed-Sieve-e

Default setup (Run "Run Lead.bat") expects 3 machines. Run "Run Lead.bat" once and "Run Follower.bat" twice. Runs on port 10001.

Custom implementation of the Sieve of Eratosthenes (https://en.wikipedia.org/wiki/Sieve_of_Eratosthenes)

Description --

Rather than listing all of the natural numbers, this sieve starts at 3 and skip counts by two. Each machine recieves an evenly sized chunk to process. If we wanted to compute the primes below 10000 between two machines, each machine would compute chunks of size 5000 (due to optimizations this is lowered to ~2500).

Machines are ordered starting at 1, with the lead machine starting at one. All other machines are followers, which report to the lead machine.

When machines mark a new prime, they report a vector of the machine number, prime position (in their chunk), and the prime. All machines with a number greater than the reporting machine recieve and process this vector. 

All communication between machines is done with sockets, with the lead computer acting as the middle man between all of the following machines.

Algorithm --

Lead:
- Mark first non-zero number in the chunk as prime
- Send prime vector to respective computers 
- Mark composites of prime
- Find next non-zero number, repeat until there is no non-zero numbers after last prime.
-> if no non-zero primes left, elect next machine in sequence as lead.

Follower:
- Wait for prime vector.
- Process prime vector, marking all composites of the prime number.
- If given an appoint sequence, check if appoint sequence is from previous machine. If it is, start sieve as Lead machine.
- Repeat.

