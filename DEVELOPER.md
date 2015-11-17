# logstash-filter-fieldmap
Takes a constant list of fields and a target field in the event.  Splits 
the event by a specified sperator and then creates a dictionary field
where the constant list are the keys and the split string is the values.
