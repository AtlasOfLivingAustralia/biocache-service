1. Export User Assertions from the old Cassandra database used by biocache-service and biocache-store.

   Requires: python3, write access to `/data/tmp`, cqlsh that defaults to old Cassandra database.
    ```
    ./export.sh
    ```
   Assertions without occurrences are included unless removal steps are uncommented in the script.

2. Copy the output file to the new server
    ```
    scp /data/tmp/qa.final.csv newServer:/data/tmp/qa.final.csv
    ```

3. Import User Assertions into the new Cassandra database on the new server.
    ```
   ./import.sh
   ``` 

   