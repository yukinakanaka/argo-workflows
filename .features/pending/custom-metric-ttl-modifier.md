Description: Set a TTL for individual custom metrics
Authors: [yukinakanaka](https://github.com/yukinakanaka)
Component: Telemetry
Issues: 12079

You can set a `ttl` modifier on a custom metric in `metricsConfig` to remove its series some time after their last update, overriding `metricsTTL` for that metric.
Use this to clear series that only matter while a workflow runs, such as a duration gauge, while keeping other custom metrics such as counters.
For example, if your Workflows define a custom metric named `workflow_duration`, `modifiers: {workflow_duration: {ttl: "30m"}}` removes its series 30 minutes after their last update.
A realtime metric is only removed once its workflow has completed.
Histogram metrics are never cleared.
