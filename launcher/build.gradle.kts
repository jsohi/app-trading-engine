dependencies {
    implementation(project(":cluster"))
    implementation(project(":gateway"))
    implementation(project(":projections"))
    implementation(project(":media-driver"))
    implementation(project(":pricing-service"))
    implementation(project(":websocket-server"))
    implementation(libs.aeron.driver)
    implementation(libs.aeron.archive)
}
