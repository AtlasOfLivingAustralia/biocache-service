1. Export User Assertions from the old Cassandra database used by biocache-service and biocache-store.

   Requires: python3, write access to `/data/tmp`, cqlsh that defaults to old Cassandra database.
    ```
    ./export.sh
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

   