# TechHub Component Deployment

## Index

- [TechHub Component Deployment](#techhub-component-deployment)
  - [Index](#index)
  - [Files overview](#files-overview)
  - [Requirements](#requirements)
  - [Resoruces Azure Devops](#resoruces-azure-devops)
    - [1. Variable groups (Library)](#1-variable-groups-library)
    - [2. Pipelines](#2-pipelines)
    - [3. Releases](#3-releases)
  - [Stages to deploy](#stages-to-deploy)
    - [1. Create library](#1-create-library)
    - [2. Create images base](#2-create-images-base)
    - [3. Create image microservice and artifact helm](#3-create-image-microservice-and-artifact-helm)
    - [4. Create artifact IaC](#4-create-artifact-iac)
    - [5. Create releases](#5-create-releases)
    - [6. Variables](#6-variables)


## Files overview

Each component has the following files and folders structure:

- **Helm**: This folder contains the templates of helm to build the charts for deploy microservices.

- **IaC**: Infrastructure as Code. This folders contains all the required files to set and deploy the services like cloud storage, queues, etc with Terraform. Besides this folder contain scripts to create, copy or other utilities to prepare the deployment of the service. Finally, it contain an azure-pipeline.yml to create 'Artifact' in Azure Devops that will later be used in the release responsible for generating all the resources.

- **imagesbase**: Contains the base images required for the component, used to accelerate creation of images dockers of microservices. This folder contain azure-pipeline.yml to create the pipeline that it create and upload imagen docker to ACR with different tags. This images download and install the libraries generated in the folder **libraries**.

- **libraries**: Contains the specific libraries and SDKs for the component and its azure-pipeline.yml. This library is saved within Azure Devops artifact feed. 

- **services**: Contains the component code files.

## Requirements

- Azure suscription
- Cluster Kubernetes
- Globals Resources

## Resoruces Azure Devops

### 1. Variable groups (Library)

All azure-pipeline.yml of the **imagesbase** and **libraries** contains variables that these variables are set by groups of variables, in Azure Devops this groups are set in Library menu.

This way, the azure-pipeline.yml does not have to be modified, in case or these variables need to modified, modify only the values of variables within group variables of Library if you must update something param.

### 2. Pipelines

This section is used to generate both the docker images and upload them to the corresponding acr as well as to package and generate Artifact, which will later be necessary to upload in the deployment releases.


### 3. Releases

This section is used to create two releases, one for IaC and another for service deployment. 

This will be the final step with which everything necessary to have the component, toolkit or solution available is created or deployed.

## Stages to deploy

### 1. Create library

Microservices GenAI need to connect with different cloud resources. To connect to these resources they use a library that implements the connectors.

So you have to compile the library and save it in a feed. You have to create the pipeline of azure within the folder **libraries** and run in a branch master to compile and stored.

Before you have to create the *Library* necesary with the correct values. You can check the name of library and values in yml.

### 2. Create images base

Microservices GenAI are created about imagenes base. This saves time when generating the final docker image of the microservice.

You have to create the pipeline with its corresponding yml within of folder **imagesbase**.

Before you have to create the *Library* necesary with the correct values. You can check the name of library and values in yml.

### 3. Create image microservice and artifact helm

Each microservices GenAI have own docker image.

To create image, in the folder **services** there is azure-pipeline.yml with configuration of pipeline to create and storage this image.

Before you have to create the *Library* necesary with the correct values.

This pipeline create docker image and artifact with helm code to deploy in the release.

### 4. Create artifact IaC

Each microservices GenAI have own IaC code in Terraform and Scripts.

To create artifact, in the folder **IaC** there is azure-pipeline.yml with configuration of pipeline to create this artifact that later you need to create release.

Before you have to create the *Library* necesary with the correct values.

### 5. Create releases

Finally, to create cloud resources and deploy services you have to create two releases. These releases can have one or more stages.

1. Release IaC: This release is in charge of creating cloud and kubernetes resources. In IaC release you have two stages:

   - Terraform

### 6. Variables

