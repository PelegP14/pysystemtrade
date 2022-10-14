from sysbrokers.IB.ib_connection import connectionIB
conn = connectionIB( 999, ib_ipaddress = "127.0.0.1", ib_port=4001) # the first compulsory value is the client_id; the keyword args are the default values and can be omitted
print(conn)
