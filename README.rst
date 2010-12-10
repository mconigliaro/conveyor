========
Conveyor
========


Introduction
------------

Conveyor is a simple continuous deployment framework built on top of `Apache
Zookeeper <http://hadoop.apache.org/zookeeper/>`_. Conveyor itself consists of
two components:

- **conveyor**: Daemon program used to watch for and deploy new applications
- **hoist**: Command line client used to manage data within ZooKeeper

Typically, the **conveyor** daemon runs on each application server, and the
**hoist** command is invoked from your continuous integration server (e.g.
`Hudson CI <http://hudson-ci.org/>`_, etc.) every time a new application is
ready to be deployed:

``$ hoist application create myapp 1.0``

This command will create/update an application node within ZooKeeper. The
**conveyor** daemons will see this change almost immediately and run the
appropriate commands to install or update the application.

Now of course, you probably don't want *all* of your application servers to
deploy the application at the same time, because that will almost certainly lead
to a brief period of downtime while the deployment is occurring. This is where
Conveyor's concept of "slots" comes in. By default the **hoist** command creates
new applications with just one slot. When an application change is detected, the
first **conveyor** daemon that sees this change will "occupy" that free slot
until the application is deployed. In the meantime,the rest of the **conveyor**
daemons will have to wait until another free slot becomes available. The result
is a staggered deployment across your entire farm.

So how does Conveyor know how to deploy your application? This is where the
**get-version-cmd** and **deploy-cmd** attributes of an application come in.
Before a **conveyor** daemon deploys an application, it compares the output of
the command specified by **get-version-cmd** to the version associated with the
application in ZooKeeper. If the versions differ, then the comand specified by
**deploy-cmd** is run. A simple example for an application distributed as a .deb
packages might look something like this:

::

  get-version-cmd: /usr/bin/dpkg-query --showformat '${Version}' --show %(id)s
  deploy-cmd: /usr/bin/aptitude install %(id)s=%(version)s

Notice the placeholders that look like **%(foo)s** in the commands above. This
is how you reference information about a particular application in ZooKeeper.


Installation
------------

#. Set up a `Zookeeper <http://hadoop.apache.org/zookeeper/>`_ server/ensamble
   (in some cases, this may be as simple as: ``aptitude install zookeeper``)
#. Install the Python bindings for ZooKeeper everywhere that **conveyor** and
   **hoist** will be run (again, this may be as simple as: ``aptitude install
   python-zookeeper``)
#. Install Conveyor (FIXME)


Configuration
-------------

FIXME


To Do
-----

- Implement proper ACLs
- Finish installation/configuration documentation
- Upload package to PyPI


Known Issues
------------

- Segfault when reconnecting to ZooKeeper (https://issues.apache.org/jira/browse/ZOOKEEPER-740)


Authors
-------

- Michael T. Conigliaro <mike [at] conigliaro [dot] org>
