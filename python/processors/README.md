# Examples: Python

This directory contains examples of processors written in Python. Below you can find a brief description of each processor.

## Aptos Tontine

This example demonstrates the simplest possible variant of a processor. There are three parts:

- Processor: Processes transactions and writes to the DB based on events.
- Database: It uses sqlite so there are no external dependencies / processes.
- API: Exposes a Flask based API for querying data from the DB.

It also demonstrates a parser function that outputs operations to create, update, and delete rows.

This processor is part of a full Aptos project, so you can see the processor in context alongside the Move module and frontend.
