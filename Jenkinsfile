pipeline {
  agent {
    node {
      label 'demo-01'
    }

  }
  stages {
    stage('test-01') {
      agent {
        node {
          label 'demo-01'
        }

      }
      steps {
        echo 'demo test 01'
      }
    }

  }
}