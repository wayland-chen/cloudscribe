# Just a Router #

A distributed data collecting systems(especially for realtime or offline logs).
features:
integration with the distributed coordinating system --- zookeeper, to provide group services.

## high scalability ##
easily horizontal expanding.<br />
## high performance ##
billions of messages per day is okay.<br />
## high flexibility ##
routing configuration is very flexible: messages transfering can use many modes such as multi-send(any|all), mem/disk buffering send.
## fault tolerant ##
if one server die, client will detect this very soon, and retry another server. if all servers in a group died, client can buffering for a while, and periodically retry.


<h3>A Typical Use case --- data aggregation across Multi-Datacenter</h3>
<img src='http://xppublic.googlegroups.com/web/cloudscribe-case1.jpg' alt='sorry' />