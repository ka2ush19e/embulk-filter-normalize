Embulk::JavaPlugin.register_filter(
  "normalize", "org.embulk.filter.NormalizeFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
