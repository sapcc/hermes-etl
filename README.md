# Hermes-ETL

[Hermes](http://github.com/sapcc/hermes) is an OpenStack audit data retrieval service for accessing CADF audit events collected through Keystone middleware.   
 
**Hermes ETL** is the pipeline component to transport audit data from Openstack via RabbitMQ into ElasticSearch


 
## Features (WIP) 

* Transport Audit Events from Openstack RabbitMQ notifications bus to Elasticsearch for use with Hermes 
* Provide configuration for Tenant specific Auditing
* Turn on and off audit pipelines from OpenStack
* Rules engine for transforming data, including attaching TenantID and/or DomainID to Audit Events
