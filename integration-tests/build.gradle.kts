dependencies {
    testImplementation(project(":launcher"))
    testImplementation(project(":query-service"))
    testImplementation(project(":test-support"))
    testImplementation(libs.artio.core)
    testImplementation(libs.aeron.test.support)
}
