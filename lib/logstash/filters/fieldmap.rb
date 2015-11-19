# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# This filter will split the src_field on delimiter and then
# create a map in the dst_field by pairing the elements of
# the keys config item with the values from the split src 
# field.
#
# If the number of elements in the split src_field and the 
# supplied keys is not the same the event will receive a tag
# and be sent along
#
class LogStash::Filters::FieldMap < LogStash::Filters::Base

  # Setting the config_name here is required. This is how you
  # configure this filter from your Logstash config.
  #
  # filter {
  #   fieldmap {
  #     src_field => 'message'
  #     dst_field => 'mapped_message'
  #     delimiter => ' '
  #     keys => ['KeyA', 'KeyB']
  #   }
  # }
  #
  config_name "fieldmap"
  
  # Field to use as source for map values
  config :src_field, :validate => :string, :default => "message"

  # Field to store processed values into.
  # this filter will overwrite any data already in dst_field
  config :dst_field, :validate => :string, :default => "mapped_message"

  # Delimiter to split src_field by
  config :delimiter, :validate => :string, :default => " "

  # List of keys to use in the dst map
  config :keys, :validate => :array, :required => true
  
  # Append value to the tag field when the mapping failed
  config :map_failure, :validate => :string, :default => "_fieldmapfailed"
  public
  def register
    # Add instance variables 
  end # def register

  public
  def filter(event)
  @logger.debug? and @logger.debug("Running fieldmap filter", :event => event)

    if event[@src_field]
      #  split the src field on delimiter then check if that length matches
      #  the key lenght, if not, explode
      split_src = event[@src_field].split(@delimiter)
      @logger.debug? and @logger.debug("split_src is: ", :split_src => split_src)
      if split_src.length == @keys.length
        event[@dst_field] = {}  #  don't need to save off the source data, already split into split_src
        idx = 0
        split_src.map do |val| 
          val = val.strip
          if val.include?('{')
            val = LogStash::Json.load(val.gsub("\\", ""))
          end
          event[@dst_field][@keys[idx]] = val 
          idx=idx+1
        end
        filter_matched(event)
      else
        event["tags"] ||= []
        event["tags"] << @map_failure unless event["tags"].include?(@map_failure)
        @logger.info? and @logger.info("Event failed field map")
      end
    end

  @logger.debug? and @logger.debug("Event now: ", :event => event)

  end # def filter
end # class LogStash::Filters::Example
