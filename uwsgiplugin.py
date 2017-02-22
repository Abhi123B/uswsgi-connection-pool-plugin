NAME='uws_conn_pool'

CFLAGS = ['-Iplugins/uws_conn_pool/libs/src -Wno-write-strings']
LDFLAGS = ['-Lplugins/uws_conn_pool/libs/libs -Lplugins/uws_conn_pool/libs/precompiled']
LIBS = ['-lstdc++ -lzmqServer -lamqpConnPool -lcommonUtils  -lamqpcpp -lrabbitmq -lzmq -lpqx -lpq -lssl -lcrypto -lpthread']
GCC_LIST = ['uws_conn_pool.cc']
