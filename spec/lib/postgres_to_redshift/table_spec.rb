require 'spec_helper'

RSpec.describe PostgresToRedshift::Table do
  context 'with a simple table' do
    before do
      attributes = { 
        "table_catalog" => "postgres_to_redshift",
        "table_schema" => "public",
        "table_name" => "films",
        "table_type" => "BASE TABLE",
        "self_referencing_column_name" => nil,
        "reference_generation" => nil,
        "user_defined_type_catalog" => nil,
        "user_defined_type_schema" => nil,
        "user_defined_type_name" => nil,
        "is_insertable_into" => "YES",
        "is_typed" => "NO"
      }

      @table = PostgresToRedshift::Table.new(attributes: attributes)
    end

    describe '#name' do
      it 'returns the name of the table' do
        expect(@table.name).to eq("films")
      end
    end

  end
end
