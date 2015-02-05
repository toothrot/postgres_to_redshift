class PostgresToRedshift::Table
  attr_accessor :attributes

  def initialize(attributes: )
    self.attributes = attributes
  end

  def name
    attributes["table_name"]
  end
end
