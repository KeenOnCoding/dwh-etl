import static com.sigmasoftware.Utils.*
import com.sigmasoftware.ConfigurationManager

def call(configName) {

    baseResourcePath = 'etl-configs'
    jobConfig = libraryResource "${baseResourcePath}/${configName}.json"
    commonConfig = libraryResource "shared.json"

    targetJobFilename = "job_configuration.json"
    targetEnvFilename = "common_configuration.json"

    confManager = new ConfigurationManager()
    confManager.createNewDirectory(configName)
    dir_path = confManager.getDirectoryPath()

    tmpJobPath = confManager.writeToFile("tmp_job.json", jobConfig)
    tmpEnvPath = confManager.writeToFile("tmp_common.json", commonConfig)

    replaceJobCommand = confManager.buildReplaceEnvVarsCommand(tmpJobPath, targetJobFilename)
    replaceEnvCommand = confManager.buildReplaceEnvVarsCommand(tmpEnvPath, targetEnvFilename)

    sh replaceJobCommand
    sh replaceEnvCommand

    final def command = buildDockerCommand('run', '--rm --network=host', dir_path, 'dwh', '${GLOBAL_DOCKER_IMAGE_TAG}', 'sql-processor-start')
    sh command
}