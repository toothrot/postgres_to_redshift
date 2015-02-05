class PostgresToRedshift::Column
  attr_accessor :attributes

  def initialize(attributes: )
    self.attributes = attributes
  end
end
