require 'helper'

class AnomalyDetectOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    tag test.anomaly
    outlier_term 28
    outlier_discount 0.05
    score_term 14
    score_discount 0.05
    tick 10
    smooth_term 3
    value y
  ]
  
  def create_driver (conf=CONFIG, tag="debug.anomaly")
    Fluent::Test::OutputTestDriver.new(Fluent::AnomalyDetectOutput, tag).configure(conf)
  end

  def test_configure
  end

  def test_emit
    require 'csv'
    reader = CSV.open("../public/stock.2432.csv", "r")
    header = reader.take(1)[0]
    p header
    d = create_driver
    d.run do 
      reader.each_with_index do |row, idx|
        d.emit({'y' => row[4].to_i})
        d.instance.flush_emit(60)
      end
      d.emit({'y' => 0})
      d.instance.flush_emit(60)
    end
  end
end
