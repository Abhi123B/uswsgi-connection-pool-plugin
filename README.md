# uswsgi-connection-pool-plugin

custom plugin for uwsgi that allows to pool connections between different endpoints.

*Added support for connection-pool to rabbitmq servers, currently can only work with default exchanges
*Added support to read configs from postgresql database

The plugin works by creating pool of AMQP connections to multiple RAbbitMQ servers , which is reads from a config file or a database. 
It publishes the messages to RMQ queues in two ways,
-> A http post request to uwsgi
-> Using a zmq dealer socket
Expects the post request data/zmq send data in following format
		connection_id|rmq_queue_name|msg_to_be_sent
example         14|queue_123|test-message

It takes a config file as input parameter plugin-cgf-file, the cfg file accents the following parameters all are comma seperated
      config			details			values			examples
   UWS_USE_DB		enable or disable db		true or false		USE_DB,true
   DB_NAME		the name of db			string			DB_NAME,my-db
   DB_USERNAME		the username			string			DB_USERNAME,db-user
   DB_PASSWORD		auth password			string			DB_PASSWORD,db-password
   DB_HOST_ADDRESS 	the host addr to conn to	ipv4 address		DB_HOST_ADDRESS,127.0.0.1
   DB_PORT		the db port			integer			DB_PORT,5674
   NO_OF_CONNECTIOS	no of connections per rmqserver	integer			NO_OF_CONNECTIOS,5
   RMQ_CONN_STR		the rabbitmq connection string	string			RMQ_CONN_STR,id,user:pass@127.0.0.1:5672/vhost
   ZMQ_SOCK_ADDRESS	the ZMQ socket address		string			ZMQ_SOCK_ADDRESS,ipc:///tmp/feeds/0

if USE_DB is false all db related configs are ignored and connection is made using RMQ_CONN_STR configs.

the RMQ_CONN_STR accepts a id which is the connection_id of post or send data, there can be multiple RMQ_CONN_STR.
every id supports a backup server , to which msgs are published if primary is down, example
RMQ_CONN_STR,14,primary_user:primary_pass@127.0.0.1:5672/virtual_host,secondary_user:secondary_pass@127.0.0.2:5672/virtual_host

sample config file and uwsgi .ini are added
libs/examples/sample.cfg libs/examples/sample.ini

Dependencies
->uwsgi --- https://github.com/unbit/uwsgi
->rabbitMqc --- https://github.com/alanxz/rabbitmq-c
->amqpcpp --- https://github.com/akalend/amqpcpp
->zmq --- https://github.com/zeromq/libzmq
->pqxx --- http://pqxx.org/download/software/libpqxx/libpqxx-4.0.tar.gz

all libraries needs to be compile with -fPIC flag , have added precompiled libraries if don't want to compile.

Installation 
-> get the uwsgi source from above link or any other uwsgi source code
-> compile and install uwsgi
-> inside uwsgi main directory look for plugins directory , mkdir uws-conn-pool
-> copy all files to created directory
-> switch to main directory, compile the plugin using
   	python uwsgiconfig.py --plugin plugins/uws_conn_pool/
-> if successful uws_conn_pool_plugin.so shoud be generated
-> copy this file to /usr/lib/uwsgi/plugins/
-> start uwsgi using a .ini file or using below command
	uwsgi --master --workers 2 --threads 2 --socket 127.0.0.1:9000 --plugin uws_conn_pool --protocol http --plugin-cfg-file pool.cfg

