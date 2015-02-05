require 'spec_helper'

RSpec.describe PostgresToRedshift do
  it 'opens a read only connection to source database' do
    read_only_state = PostgresToRedshift.source_connection.exec("SHOW transaction_read_only").first["transaction_read_only"]

    expect(read_only_state).to eq("on")
  end

  context 'with a simple table' do
    before do
      PostgresToRedshift::Test.test_connection.exec(%Q[DROP TABLE IF EXISTS "films"; CREATE TABLE IF NOT EXISTS "films" ("id" SERIAL PRIMARY KEY, "title" text);])
    end

    it 'lists available tables' do
      expect(PostgresToRedshift.new.tables.size).to eq(1)
      expect(PostgresToRedshift.new.tables.first.name).to eq("films")
    end

    it 'lists column definitions' do
      table = PostgresToRedshift.new.tables.first
      film_columns = PostgresToRedshift.new.column_definitions(table)

      expect(film_columns.to_a.size).to eq(2)
      expect(film_columns.first["column_name"]).to eq("id")
      expect(table.columns.first.name).to eq("id")
    end
  end
end
