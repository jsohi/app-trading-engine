val aeronVersion: String by project

dependencies {
    implementation(project(":messages"))
    implementation("io.aeron:aeron-client:$aeronVersion")
}
