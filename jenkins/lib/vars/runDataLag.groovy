import com.sigmasoftware.ConfigurationManager
import groovy.json.JsonOutput
import static com.sigmasoftware.Utils.*

def call(Map overrides = [:]) {
    baseResourcePath = "utility-config/data-lag-configs"
    config = libraryResource "${baseResourcePath}/data-lag.json"
    rules = libraryResource "${baseResourcePath}/rules.yaml"
    commonConfig = libraryResource "shared.json"

    configMap = readJSON text: "${config}"
    utilityConfig =  configMap['data_lag'] + ["rules": rules]

    configuration = Map.ofEntries(
            Map.entry("data_lag", utilityConfig)
    )

    jsonUtilityConfig = JsonOutput.toJson(configuration)

    targetJobFilename = "utility_configuration.json"
    targetEnvFilename = "common_configuration.json"

    confManager = new ConfigurationManager()
    confManager.createNewDirectory("data-lag")
    dir_path = confManager.getDirectoryPath()

    tmpJobPath = confManager.writeToFile("tmp_job.json", jsonUtilityConfig)
    tmpEnvPath = confManager.writeToFile("tmp_common.json", commonConfig)

    job_path = "${dir_path}/utility_configuration.json"
    commons_path = "${dir_path}/common_configuration.json"

    replaceJobCommand = confManager.buildReplaceEnvVarsCommand(tmpJobPath, targetJobFilename)
    replaceEnvCommand = confManager.buildReplaceEnvVarsCommand(tmpEnvPath, targetEnvFilename)

    sh replaceJobCommand
    sh replaceEnvCommand

    final def command = buildDockerCommand('run', '--rm --network=host', dir_path, 'dwh', '${GLOBAL_DOCKER_IMAGE_TAG}', 'data-lag-start')
    sh command
}