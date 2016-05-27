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

  # Regex to split src_field by
  config :regex, :validate => :string, :default => '[[:space:]]'

  # Regroup things that were unwantedly split by the regex
  config :text_qualifier, :validate => :string, :default => false

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
      split_src = event[@src_field].split(/#{@regex}/)
      @logger.debug? and @logger.debug("split_src is: ", :split_src => split_src)

      if @text_qualifier
        split_src_new_idx = split_src_orig_idx = 0
        qualifier_start_idx = -1
        split_src.map do |val|

          val = val.strip   # this will cause problems if the text_qualifier is a space character

          # Handle the case where we're in a qualified string
          if val.start_with? @text_qualifier
            qualifier_start_idx = split_src_orig_idx
          elsif val.end_with? @text_qualifier
            # TODO make this put in whatever it found to split on... not just a space
            val = split_src[qualifier_start_idx..split_src_orig_idx].join(' ')
            val = val.tr(@text_qualifier, "") # strips off the qualifier marks
            qualifier_start_idx = -1
          end

          # this is the "not in the middle of a qualified string" (a.k.a normal) case
          if qualifier_start_idx < 0
            split_src[split_src_new_idx] = val
            split_src_new_idx += 1
          end

          split_src_orig_idx += 1

          # Handle the case where there is no "end" to the qualified string
          if split_src_orig_idx >= split_src.length
            event["tags"] ||= []
            event["tags"] << @map_failure unless event["tags"].include?(@map_failure)
            @logger.info? and @logger.info("Event failed field map")
          end
        end

        # cleanup the extra from the end now
        split_src_new_idx -= 1
        split_src_orig_idx -= 1
        while split_src_orig_idx > split_src_new_idx do
          split_src.delete_at(split_src_orig_idx)
          split_src_orig_idx -= 1
        end
      end

      if split_src.length == @keys.length
        event[@dst_field] = {}  #  don't need to save off the source data, already split into split_src
        idx = 0
        split_src.map do |val|
          val = val.strip
          if val.include?('{')
            begin
              val = LogStash::Json.load(val.gsub("\\", ""))
            rescue LogStash::Json::ParserError  # if its not valid json leave it alone
            end
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
