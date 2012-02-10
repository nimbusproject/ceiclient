#############################################################
 CEIclient - Command line tools for controlling CEI services
#############################################################

`CEIclient` offers command line tools to control the CEI services of the OOI CI
infrastructure.

It currently consists of one command line program named `ceictl`, capable of
controlling the following services:

* the Provisioner
* the EPU Manager
* the Process Dispatcher

Usage
#####

The general usage of `ceictl` is:

    ``ceictl [options] <service> <command> <command arguments>``

To interact with CEI services, `ceictl` uses an AMQP messaging system
(specifically, a RabbitMQ deployment).
Therefore, a user must provide the necessary variables to connect to this
system.
Three values are required:

* a RabbitMQ broker address (hostname or IP),
* a RabbitMQ username,
* its corresponding RabbitMQ password.

A typical `ceictl` invocation will look like this:

    ``ceictl -b $RABBITMQ_HOST -u $RABBITMQ_USERNAME -p $RABBITMQ_PASSWORD <service> <command> <command arguments>``

Provisioner
-----------

To control the Provisioner, use the ``provisioner`` service name.

To provision a node with the `sleeper` DT in the Amazon EC2 US East region using
a `t1.micro` instance type and the provisioning variables provided in the
`examples/provisioning_vars.json` file:

    ``ceictl provisioner provision sleeper ec2-east t1.micro examples/provisioning_vars.json``

To describe nodes `node1` and `node2`:

    ``ceictl provisioner describe node1 node2``

To describe all nodes:

    ``ceictl provisioner describe``

To terminate all nodes and shut down the provisioner:

    ``ceictl provisioner terminate_all``

EPU Manager
-----------

To control the EPU manager, use the ``epu`` service name.

To list all EPUs:

    ``ceictl epu list``

To describe the EPU named `epu1`:

    ``ceictl epu describe epu1``

To reconfigure the EPU named `epu1` and change its engine configuration
parameter `preserve_n` to the integer value 2:

    ``ceictl epu reconfigure --int engine_conf.preserve_n=2 epu1``

Boolean and string values can also be provided with the ``--bool`` and
``--string`` options.

Process Dispatcher
------------------

To control the Provisioner, use the ``process`` service name.

To dispatch a process for execution using the process specification in file `examples/process_spec.yml`:

    ``ceictl process dispatch examples/process_spec.yml``

To describe all processes:

    ``ceictl list``

To describe the process with PID `pid1`:

    ``ceictl process describe pid1``

To terminate the process with PID `pid1`:

    ``ceictl process kill pid1``
