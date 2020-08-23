pipeline {
  agent {dockerfile true}
  stages {
    stage('build') {
      steps {
        script{
        echo 'pipeline template'
        sh "pip3 install --dev"
        sh "pytest"
        }
      }
    }
  }
}
