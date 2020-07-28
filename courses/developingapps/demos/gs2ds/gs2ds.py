from google.cloud import datastore
import apache_beam as beam
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.datastore.v1new.types import Entity
from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore

project = '[GCLOUD_PROJECT]'  # Replace [GCLOUD_PROJECT] with project
kind = 'President'

datastore_client = datastore.Client(project)

options = PipelineOptions(project=project)
p = beam.Pipeline(options=options)
lines = p | 'Read from Cloud Storage' >> beam.io.ReadFromText('gs://[GCLOUD_BUCKET]/usa_presidents.csv')  # Replace [GCLOUD_BUCKET] with Cloud Storage bucket


def to_entity(line):
    fields = line.split(',')  # id,president,startYear,endYear,party,homeState,dateOfBirth
    id = fields[0]
    president = fields[1]
    names = president.split(' ')
    firstName = names[0]
    lastName = names[1]
    startYear = fields[2]
    endYear = fields[3]
    party = fields[4]
    homeState = fields[5]
    dateOfBirth = fields[6]

    key = datastore_client.key(kind, str(id))
    entity = datastore.Entity(key=key)
    props = {
        'firstName': unicode(firstName),
        'lastName': unicode(lastName),
        'startYear': int(startYear),
        'endYear': int(endYear),
        'party': unicode(party),
        'homeState': unicode(homeState),
        'dateOfBirth': datetime.strptime(dateOfBirth, '%Y-%m-%d')
    }
    entity.update(props)
    return Entity.from_client_entity(entity)


entities = lines | 'To Entity' >> beam.Map(to_entity)
entities | 'Write To Datastore' >> WriteToDatastore(project)
# lines | 'Write to Cloud Storage' >> beam.io.WriteToText('gs://[GCLOUD_BUCKET]/out')

p.run().wait_until_finish()
