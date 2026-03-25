val aeronVersion: String by project

dependencies {
    implementation(project(":cluster"))
    implementation(project(":gateway"))
    implementation(project(":projections"))
    implementation(project(":media-driver"))
    implementation(project(":pricing-service"))
    implementation(project(":websocket-server"))
    implementation("io.aeron:aeron-driver:$aeronVersion")
    implementation("io.aeron:aeron-archive:$aeronVersion")
}
