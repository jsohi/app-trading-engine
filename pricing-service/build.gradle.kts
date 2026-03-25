val aeronVersion: String by project
val agronaVersion: String by project

dependencies {
    implementation(project(":messages"))
    implementation("io.aeron:aeron-client:$aeronVersion")
    implementation("org.agrona:agrona:$agronaVersion")
}
