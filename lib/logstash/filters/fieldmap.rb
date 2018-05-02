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

  # Regroup things that were unwantedly split by the regex. This value can not be whitespace(s).
  config :text_qualifier, :validate => :string, :default => false

  # List of keys to use in the dst map
  config :keys, :validate => :array, :required => true

  # Append value to the tag field when the mapping failed
  config :map_failure, :validate => :string, :default => "_fieldmapfailed"
  config :invalid_text_qualifier_failure, :validate => :string, :default => "_fieldmap_invalid_text_qualifier"
  config :unmatched_text_qualifier_failure, :validate => :string, :default => "_fieldmap_unmatched_text_qualifier"

  public
  def register
    # Add instance variables
  end # def register

  public
  def filter(event)
  @logger.debug? and @logger.debug("Running fieldmap filter", :event => event)

    if event.get(@src_field)
      #  split the src field on delimiter then check if that length matches
      #  the key lenght, if not, explode
      split_src = event.get(@src_field).split(/#{@regex}/)
      @logger.debug? and @logger.debug("split_src is: ", :split_src => split_src)

      if @text_qualifier
        # protecting ourselves against bad things that could happen
        if @text_qualifier.strip.length == 0
            add_tag(event, @invalid_text_qualifier_failure)
        end

        split_src_new_idx = split_src_orig_idx = 0
        qualifier_start_idx = -1
        split_src.map do |val|

          val = val.strip

          # Handle the case where we're in a qualified string
          if val.end_with? @text_qualifier

            # We caught an string "ending with" a quote before it starts with one. ruh roh.
            if qualifier_start_idx < 0
              add_tag(event, @unmatched_text_qualifier_failure)
            end

            # TODO make this put in whatever it found to split on... not just a space
            val = split_src[qualifier_start_idx..split_src_orig_idx].join(' ')
            val = val.tr(@text_qualifier, "") # strips off the qualifier marks
            qualifier_start_idx = -1
          elsif val.start_with? @text_qualifier
            qualifier_start_idx = split_src_orig_idx
          end

          # this is the "not in the middle of a qualified string" (a.k.a normal) case
          if qualifier_start_idx < 0
            split_src[split_src_new_idx] = val
            split_src_new_idx += 1
          end

          split_src_orig_idx += 1

          # Handle the case where there is no "end" to the qualified string
          if split_src_orig_idx >= split_src.length
            add_tag(event, @unmatched_text_qualifier_failure)
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
        event.set(@dst_field, {}) #  don't need to save off the source data, already split into split_src
        idx = 0
        split_src.map do |val|
          val = val.strip
          if val.include?('{')
            begin
              val = LogStash::Json.load(val.gsub("\\", ""))
            rescue LogStash::Json::ParserError  # if its not valid json leave it alone
            end
          end
          if val.nil?
            val = ""
          end
          event.set("[@dst_field][@keys[idx]]", val)
          idx=idx+1
        end
        filter_matched(event)
      else
        add_tag(event, @map_failure)
      end
    end

  @logger.debug? and @logger.debug("Event now: ", :event => event)

  end # def filter

  ##
  ## Private methods below here
  ##
  private

  def add_tag(event, tag)
    if (event.get("tags").nil? || event.get("tags") == false)
        event.set("tags", [])
    end
    unless event.get("tags").include?(tag)
        event.set("tags", (event.get("tags") << tag)) #<< tag unless event.get("tags").include?(tag)
    end
    @logger.info? and @logger.info("Event failed field map: " + tag)
  end

end # class LogStash::Filters::Example
