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
          delimiter => ' '
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
end
