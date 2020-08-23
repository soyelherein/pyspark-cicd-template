pipeline {
  agent any
  stages {
    stage('build') {
      steps {
      	def dockerHome = tool name: 'local-docker', type: 'dockerTool'
      	def dockerCMD = "${dockerHome}/bin/docker"
        echo 'pipeline template'
        sh "${dockerCMD} build -t xxx"
      }
    }
  }
}
