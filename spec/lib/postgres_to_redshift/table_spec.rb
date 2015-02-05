require 'spec_helper'

RSpec.describe PostgresToRedshift::Table do
  context 'with a simple table' do
    before do
      attributes = { 
        "table_catalog" => "postgres_to_redshift",
        "table_schema" => "public",
        "table_name" => "films",
        "table_type" => "BASE TABLE",
      }
      columns = [
        {
          "table_catalog"            => "postgres_to_redshift",
          "table_schema"             => "public",
          "table_name"               => "films",
          "column_name"              => "description",
          "ordinal_position"         => "2",
          "column_default"           => nil,
          "is_nullable"              => "YES",
          "data_type"                => "character varying",
          "character_maximum_length" => "255",
          "character_octet_length"   => "1020"
        }
      ]

      @table = PostgresToRedshift::Table.new(attributes: attributes, columns: columns)
    end

    describe '#name' do
      it 'returns the name of the table' do
        expect(@table.name).to eq("films")
      end
    end

    describe '#columns' do
      it 'returns a list of columns' do
        expect(@table.columns.size).to eq(1)
        expect(@table.columns.first.name).to eq("description")
      end
    end
  end
end
