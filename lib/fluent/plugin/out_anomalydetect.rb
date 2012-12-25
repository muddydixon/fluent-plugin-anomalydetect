class Fluent::AnomalyDetectOutput < Fluent::Output
  Fluent::Plugin.register_output('anomalydetect', self)

  def configure (conf)
    super
  end
  def start
    super
  end
  def shutdown
  end
end
