import com.sigmasoftware.ConfigurationManager
import groovy.json.JsonOutput
import static com.sigmasoftware.Utils.*

def call(configName) {

    baseResourcePath = "utility-config"
    configs = libraryResource "${baseResourcePath}/dimension-cleanup.json"
    commonConfig = libraryResource "shared.json"

    Map configMap = readJSON text: "${configs}"

    dimensionMap = Map.ofEntries(
            Map.entry("dimension_cleanup", configMap['dimension_cleanup'][configName])
    )
    dimensionConfig = JsonOutput.toJson(dimensionMap)

    targetJobFilename = "utility_configuration.json"
    targetEnvFilename = "common_configuration.json"

    confManager = new ConfigurationManager()
    confManager.createNewDirectory(configName)
    dir_path = confManager.getDirectoryPath()

    tmpJobPath = confManager.writeToFile("tmp_job.json", dimensionConfig)
    tmpEnvPath = confManager.writeToFile("tmp_common.json", commonConfig)

    replaceJobCommand = confManager.buildReplaceEnvVarsCommand(tmpJobPath, targetJobFilename)
    replaceEnvCommand = confManager.buildReplaceEnvVarsCommand(tmpEnvPath, targetEnvFilename)

    sh replaceJobCommand
    sh replaceEnvCommand

    final def command = buildDockerCommand('run', '--rm --network=host', dir_path, 'dwh', '${GLOBAL_DOCKER_IMAGE_TAG}', 'dimension-cleanup-start')
    sh command
}