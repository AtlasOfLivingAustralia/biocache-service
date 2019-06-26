FROM solr:8.1.1
COPY --chown=solr:solr biocache /var/solr/data/biocache
COPY --chown=solr:solr solr.xml /var/solr/data/solr.xml
COPY --chown=solr:solr zoo.cfg /var/solr/data/zoo.cfg
COPY --chown=solr:solr lib/*.jar /opt/solr/server/solr-webapp/webapp/WEB-INF/lib/
COPY solr.in.sh /etc/default/solr.in.sh
