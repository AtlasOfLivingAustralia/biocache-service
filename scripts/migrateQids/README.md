1. Export User Assertions from the old Cassandra database used by biocache-service and biocache-store.

   Requires: groovy, write access to `/data/tmp`, `cqlsh` that defaults to old Cassandra database.
    ```
    ./export.sh
    ```
   
   Install groovy using sdkman

   ```aidl
   curl -s get.sdkman.io | bash
   source "/root/.sdkman/bin/sdkman-init.sh"
   sdk install groovy
   ```

2. Copy the output file to the new server
    ```
    scp /data/tmp/qid.csv newServer:/data/tmp/qid.csv
    ```

3. Import User Assertions into the new Cassandra database on the new server

   Edit `~/.cqlshrc`, add
   ```
   [csv]
   field_size_limit = 1000000000
   ```
   
   Run
    ```
   ./import.sh
   ``` 

Note: The default csv row size limit need to be changed otherwise some rows can't be loaded and then when 1000 rows failed to load the whole process aborts. Check this https://docs.datastax.com/en/dse/5.1/cql/cql/cql_reference/cqlsh_commands/cqlshCqlshrc.html#cqlshCqlshrc__cqlshrcLimitSize to see how to change size limit. <Br/>Basically:
1. `cp cqlshrc.sample ~/.cassandra/` (copy the cqlshrc.sample from Cassandra conf folder to your ~/.cassandra folder)
2. edit cqlshrc file. Remove the semi-colon to uncomment `field_size_limit` and give it a new value which is large enough
3. launch cqlsh with `cqlsh --cqlshrc="~/.cassandra"`


   
