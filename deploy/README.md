# Deployment

This directory contains the resources to deploy the Azure Kubernetes Service cluster and Dask-enabled JupyterHub used during the workshop.

## Infrastructure

The `terraform` directory has the terraform code for creating the AKS cluster. We make the AKS cluster (and supporting resources like the virtual network) in the `aks.tf` module. We have a cluster with

- A fixed-size node pool for core AKS pods and the JuptyerHub server
- An autoscaling `users` node pool for each user's notebook server (and Dask schedulers)
- An autoscaling worker node pool with low-priority nodes for Dask workers.

After setup, this can be deployed with `terraform apply`.

## JupyterHub

We use the [DaskHub](https://github.com/dask/helm-chart/blob/main/daskhub/README.md) Helm Chart to deploy Dask Gateway and JupyterHub. The configuration is in `gateway-config.yaml` and `secrets.yaml`. The secrets file has a structure like

```yaml
jupyterhub:
  hub:
    config:
      DummyAuthenticator:
        password: "<password>"
```

This can be deployed with `make daskhub`.