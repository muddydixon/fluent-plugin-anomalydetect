require 'helper'

class AnomalyDetectOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    tag test.anomaly
    outlier_term 28
    outlier_discount 0.05
    score_term 28
    score_discount 0.05
    tick 10
    smooth_term 3
    target y
  ]
  
  def create_driver (conf=CONFIG, tag="debug.anomaly")
    Fluent::Test::OutputTestDriver.new(Fluent::AnomalyDetectOutput, tag).configure(conf)
  end

  def test_configure
    d = create_driver('')
    assert_equal 28, d.instance.outlier_term
    assert_equal 0.05, d.instance.outlier_discount
    assert_equal 7, d.instance.smooth_term
    assert_equal 14, d.instance.score_term
    assert_equal 0.1, d.instance.score_discount
    assert_equal 300, d.instance.tick
    assert_nil d.instance.target
    assert_equal 'anomaly', d.instance.tag
    assert d.instance.record_count

    d = create_driver
    assert_equal 28, d.instance.outlier_term
    assert_equal 0.05, d.instance.outlier_discount
    assert_equal 3, d.instance.smooth_term
    assert_equal 28, d.instance.score_term
    assert_equal 0.05, d.instance.score_discount
    assert_equal 10, d.instance.tick
    assert_equal "y", d.instance.target
    assert_equal 'test.anomaly', d.instance.tag
    assert !d.instance.record_count

    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        outlier_discount 1.3
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        score_discount 1.3
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        outlier_discount -0.3
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        score_discount -0.3
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        outlier_term 0
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        score_term 0
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        smooth_term 0
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        tick 0
      ]
    }
  end

  def test_array_init
    d = create_driver
    assert_equal [], d.instance.outliers
    assert_nil d.instance.records  # @records is initialized at start, not configure
  end

  def test_sdar
    d = create_driver
    assert_instance_of Fluent::ChangeFinder, d.instance.outlier
    assert_instance_of Fluent::ChangeFinder, d.instance.score
  end

  def test_emit_record_count
    d = create_driver %[
      tag test.anomaly
      outlier_term 28
      outlier_discount 0.05
      score_term 28
      score_discount 0.05
      tick 10
      smooth_term 3
    ]

    data = (0..10).map do || (rand * 100).to_i end
    data.push 0
    d.run do 
      data.each do |val|
        (0..val - 1).each do ||
          d.emit({'y' => 1})
        end
        r = d.instance.flush
        assert_equal val, r['target']
      end
    end
  end

  def test_emit_stock_data
    require 'csv'
    reader = CSV.open("test/stock.2432.csv", "r")
    header = reader.take(1)[0]
    d = create_driver
    d.run do 
      reader.each_with_index do |row, idx|
        break if idx > 5
        d.emit({'y' => row[4].to_i})
        r = d.instance.flush
        assert r['target']
        assert r['outlier']
        assert r['score']
      end
    end
  end
end
