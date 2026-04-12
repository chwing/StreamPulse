FROM grafana/grafana:10.2.0

# Install Trino datasource plugin used by provisioned dashboard panels.
RUN grafana cli --pluginsDir "/var/lib/grafana/plugins" plugins install trino-datasource

# Copy provisioning configs so Grafana auto-configures on startup.
# No manual UI setup required.
COPY provisioning /etc/grafana/provisioning
