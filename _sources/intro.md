# National Weather Model data on Azure

This collection of notebooks was presented at the 2023 [CIROH Training and
Developers Conference](https://ciroh.ua.edu/devconference/). We'll cover the
basics of accessing National Water Model data from Azure using tools from the
[Pangeo] ecosystem and the [Planetary Computer][pc].

## Goals

We hope you'll get a few things out of this workshop:

1. Learn about the massive amount of data available on Azure, including the
   National Water Model, that's available to you. (See the [Planetary Computer
   catalog][catalog] for an overview).
2. Learn some of the basics of cloud-native geospatial, and how to best use
   these datasets from Azure.
3. Learn how to use tools from the [Pangeo] ecosystem to work with large
   geospatial datasets on the cloud.
4. Learn how the Planetary Computer enables efficient access to these datasets.

## Accessing the Compute: <https://aka.ms/ciroh-hub>

This workshop is intended to be interactive! While you can just follow along, we
really do encourage you to work through the materials along with us. We've
deployed a JupyterHub on [Azure Kubernetes Service][aks], so you have fast
access to the data. Click the button below to launch your notebook server. Pick
a unique username and use the password we share at the conference to log in.

<div style="text-align:center">
  <a class="btn btn-primary" href="https://ciroh.eastus.cloudapp.azure.com/" role="button">Launch Notebook Server</a>
</div>

Note that the JupyterHub we're using for this workshop will go away at the end
of the day. You can deploy your [own Hub][hub], or use one of the other compute
services on Microsoft to access the data. If you're not sure whether you have
access to Azure, reach out and we'll help you.

## Background

Attendees will get the most out of this workshop if they have *some* knowledge
of Python, [pandas] and [xarray], but we'll do our best to introduce the
concepts. You don't need any knowledge of cloud computing or parallel computing
with Dask.

If you're new to Python, xarray, or Jupyter, the [Pythia Foundations][foundations] book has a good introduction.

[catalog]: https://planetarycomputer.microsoft.com/catalog
[foundations]: https://foundations.projectpythia.org/landing-page.html
[pandas]: https://pandas.pydata.org/]
[Pangeo]: https://pangeo.io
[pc]: https://planetarycomputer.microsoft.com/
[xarray]: https://xarray.pydata.org/
[aks]: https://learn.microsoft.com/en-us/azure/aks/
[hub]: https://planetarycomputer.microsoft.com/docs/concepts/hub-deployment/
