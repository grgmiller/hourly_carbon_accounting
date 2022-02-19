# General Information

Title of Project: Hourly accounting of carbon emissions from electricity consumption

About: Carbon accounting is important for quantifying the sources of greenhouse gas (GHG) emissions that are driving climate change, and is increasingly being used to guide policy, investment, business, and regulatory decisions. The current practice for accounting emissions from consumed electricity, guided by standards like the GHG Protocol, uses annual-average grid emission factors, although previous studies have shown that grid carbon intensity varies across seasons and hours of the day. Previous case studies have shown that annual-average carbon accounting can bias emission inventories, but none have shown that this bias is substantial or widespread. This study addresses this gap by calculating emission inventories for thousands of residential, commercial, industrial, and agricultural facilities across the U.S., and explores the magnitude and direction of this bias compared to hourly accounting of emissions. Our results show that annual-average accounting can over- or under-estimate carbon inventories as much as 30% in certain settings but result in effectively no bias in others. Bias will be greater in regions with high variation in carbon intensity, and for end-users with high variation in their electricity consumption across hours and seasons.  As variation in carbon intensity continues to grow with growing shares of variable and intermittent renewable generation, these biases will only continue to worsen in the future. In most cases, using monthly-average emission factors does not substantially reduce bias compared to annual averages. Thus, the authors recommend that hourly accounting be adopted as the best practice for emissions inventories of consumed electricity.   


Corresponding Author:  
  Gregory J. Miller, University of California Davis, grmiller@ucdavis.edu

Contributors:

- Gregory J. Miller, University of California, Davis
- Kevin Novan, University of California, Davis,
- Alan Jenn, University of California, Davis, 

Date(s) of data collection (single date, range, approximate date): 2020-2021

SHARING/ACCESS INFORMATION

Licenses/restrictions placed on the data: MIT License

This repository contains code to reproduce the results in the paper "Hourly Accounting of Carbon Emissions from Consumed Electricity", by Gregory J. Miller, Kevin Novan, and Alan Jenn. Environmental Research Letters. (in review)


# Repository Overview

#### code
All jupyter notebooks are numbered in the order in which they should be run

#### data:
- downloaded:  files that were directly downloaded from an online source without further modification. This includes data from:
  - EPA eGRID database
  - EIA-930 (Hourly Electricity Grid Monitor)
  - NREL End-Use Load Profiles for the U.S. Building Stock
  - GIS shapefiles
- processed: any files that were created using one of the python scripts in this repository. This includes
  - Emissions factors downloaded and formatted from the singularity API
  - Cleaned emissions factor data
  - GIS shapefiles
  - NREL EULP hourly demand profiles
- manual: any files that were manually edited by the author and cannot be directly reproduced by the python scripts

# Setup
### Install Conda and Python
Download and install Miniconda from https://docs.conda.io/en/latest/miniconda.html or Anaconda from https://www.anaconda.com/distribution . We recommend using the 64-bit version with Python >3.7. Anaconda and Miniconda install a similar environment, but Anaconda installs more packages by default and Miniconda installs them as needed.
### Install a code editor 
A code editor, also known as an integrated development environment (IDE) will be required to open and run the Jupyter Notebooks used in this model. We use Visual Studio Code (download: https://code.visualstudio.com/), because it allows you to open Jupyter notebooks, and it is free, but most IDEs will work.
### Install and setup Git
After installing Anaconda or Miniconda, open an Anaconda Command Prompt (Windows) or Terminal.app (Mac) and type the following command:

```
conda install git
```

Or you can install Git Bash from https://git-scm.com/downloads

Then you will need to open Git Bash and set up git following these instructions: https://docs.github.com/en/get-started/quickstart/set-up-git

### Download the repository to your computer

Then, in a terminal window or Anaconda command prompt Anaconda command prompt, use the cd and mkdir commands to create and/or enter the directory (e.g. "Users/myusername/GitHub") where you would like to store the repository. Then run:

```
git clone https://github.com/grgmiller/hourly_average_efs.git
```

### Setup the conda environment

This will install all of the package dependencies needed to run the code. Use cd to navigate to the directory where your local files are stored (e.g. "GitHub/hourly_average_efs")

```
conda env create -f emissions.yml
```

### Run the notebooks and download data

Due to the size of the NREL dataset, running this code will require at least 32GB of RAM (64GB recommended) to avoid Memory Errors.

Run each of the notebooks in `code` in their numbered order. This repository does not contain any of the input data files. Each notebook in `code` describes the source for each data file so that the user can download these files on their local machine. 