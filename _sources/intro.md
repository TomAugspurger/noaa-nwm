# National Weather Model data on Azure

This collection of notebooks was presented at the 2023 [CIROH Training and
Developers Conference](https://ciroh.ua.edu/devconference/). We'll cover the
basics of accessing National Water Model data from Azure using tools from the
[Pangeo] ecosystem and the [Planetary Computer][pc].

## Accessing the Compute

We'll be working with some large datasets. To ensure things work well, we'll be using a JupyterHub
deployed next to the data in Azure's East US region.

<div style="text-align:center">
  <a class="btn btn-primary" href="https://ciroh.eastus.cloudapp.azure.com/" role="button">Launch Notebook Server</a>
</div>

Note that the JupyterHub we're using for this workshop will go away at the end of the day. You can
deploy your own Hub, or sign up for the Microsoft [Planetary Computer][pc] to get a similar experience.

## Background

Attendees will get the most out of this workshop if they have *some* knowledge
of [xarray], but we'll do our best to introduce the concepts. You don't need any
knowledge of cloud computing or parallel computing with Dask.

If you're new to Python, xarray, or Jupyter, the [Pythia Foundations][foundations] book has a good introduction.

[pc]: https://planetarycomputer.microsoft.com/
[Pangeo]: https://pangeo.io
[foundations]: https://foundations.projectpythia.org/landing-page.html
[xarray]: https://xarray.pydata.org/