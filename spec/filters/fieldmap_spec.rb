# encoding: utf-8
require 'spec_helper'
require "logstash/filters/fieldmap"

describe LogStash::Filters::FieldMap do
  describe "Hello World Demo" do
    let(:config) do <<-CONFIG
      filter {
        fieldmap {
          src_field => 'message'
          dst_field => 'mapped_message'
          regex => '[[:blank:]]'
          keys => ['Greeting', 'Target']
        }
      }
    CONFIG
    end

    sample("message" => "Hello World") do
      expect(subject).to include("message")
      expect(subject['mapped_message']).to eq({'Greeting' => 'Hello', 'Target' => 'World'})
    end

    sample("message" => "MazelTof {\"Hello\":\"World\"}") do
      expect(subject['mapped_message']).to eq({'Greeting' => 'MazelTof', 'Target' => {'Hello' => 'World'}})
    end
  end

  describe "Hello World Hate" do
    let(:config) do <<-CONFIG
      filter {
        fieldmap {
          src_field => 'message'
          dst_field => 'mapped_message'
          keys => ['Greeting', 'Target']
        }
      }
    CONFIG
    end

    sample("message" => "Hello Haters") do
      expect(subject).to include("message")
      expect(subject['mapped_message']).to eq({'Greeting' => 'Hello', 'Target' => 'Haters'})
    end
  end

  describe "should fail on field to value mismatch" do
    let(:config) do <<-CONFIG
      filter {
        fieldmap {
          src_field => 'message'
          dst_field => 'mapped_message'
          keys => ['timestamp', 'log']
        }
      }
    CONFIG
    end

    sample("message" => '"2016-05-26 05:00:00PST" jibberish') do
      expect(subject).to include("message")
      expect(subject['tags']).to eq(["_fieldmapfailed"])
    end
  end

  describe "should successfully allow \" to quote strings" do
    let(:config) do <<-CONFIG
      filter {
        fieldmap {
          src_field => 'message'
          dst_field => 'mapped_message'
          text_qualifier => '"'
          keys => ['timestamp', 'log']
        }
      }
    CONFIG
    end

    sample("message" => '"2016-05-26 05:00:00PST" jibberish') do
      expect(subject).to include("message")
      expect(subject['mapped_message']).to eq({'timestamp' => '2016-05-26 05:00:00PST', 'log' => 'jibberish'})
    end
  end

  describe "should successfully allow qualification on several strings" do
    let(:config) do <<-CONFIG
      filter {
        fieldmap {
          src_field => 'message'
          dst_field => 'mapped_message'
          text_qualifier => '"'
          keys => ['timestamp', 'log', 'log2']
        }
      }
    CONFIG
    end

    sample("message" => '"2016-05-26 05:00:00PST this is more words" jibberish "ron swanson"') do
      expect(subject).to include("message")
      expect(subject['mapped_message']).to eq({'timestamp' => '2016-05-26 05:00:00PST this is more words', 'log' => 'jibberish', 'log2' => 'ron swanson'})
    end
  end

  describe "should fail on field to unmatched ' mismatch" do
    let(:config) do <<-CONFIG
      filter {
        fieldmap {
          src_field => 'message'
          dst_field => 'mapped_message'
          text_qualifier => "'"
          keys => ['timestamp', 'log']
        }
      }
    CONFIG
    end

    sample("message" => "'2012-03-03 01:10:01' 'Do not 'touch that dial!'") do
      expect(subject).to include("message")
      expect(subject['tags']).to eq(["_fieldmap_unmatched_text_qualifier"])
    end
  end



end
