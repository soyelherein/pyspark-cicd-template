pipeline {
  agent any
  stages {
    stage('build') {
      	def dockerHome = tool name: 'docker', type: 'dockerTool'
      	def dockerCMD = "${dockerHome}/bin/docker"
        echo 'pipeline template'
        sh "${dockerCMD} build -t xxx"
    }
  }
}
