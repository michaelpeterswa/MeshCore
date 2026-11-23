#include <Arduino.h>   // needed for PlatformIO
#include <Mesh.h>

#include "MyMesh.h"

#ifdef ETHERNET_ENABLED
  #define ETHERNET_CLI_BANNER "MeshCore Room Server CLI"
  #include <helpers/nrf52/EthernetCLI.h>
#endif

#ifdef DISPLAY_CLASS
  #include "UITask.h"
  static UITask ui_task(display);
  static bool display_ready = false;
#endif

StdRNG fast_rng;
SimpleMeshTables tables;
MyMesh the_mesh(board, radio_driver, *new ArduinoMillis(), fast_rng, rtc_clock, tables);

void halt() {
  while (1) ;
}

static char command[MAX_POST_TEXT_LEN+1];
#ifdef ETHERNET_ENABLED
static char ethernet_command[MAX_POST_TEXT_LEN+1];
#endif

#if defined(WITH_MQTT_BRIDGE_ETHERNET)
static void startMainLoopTask();
#endif

void setup() {
  Serial.begin(115200);
  delay(1000);

  board.begin();

#ifdef HAS_EXTERNAL_WATCHDOG
  external_watchdog.begin();
#endif

#ifdef DISPLAY_CLASS
  display_ready = display.begin();
  if (display_ready) {
    display.startFrame();
    display.setCursor(0, 0);
    display.print("Please wait...");
    display.endFrame();
  }
#endif

  if (!radio_init()) { halt(); }

  fast_rng.begin(radio_driver.getRngSeed());

  FILESYSTEM* fs;
#if defined(NRF52_PLATFORM)
  InternalFS.begin();
  fs = &InternalFS;
  IdentityStore store(InternalFS, "");
#elif defined(RP2040_PLATFORM)
  LittleFS.begin();
  fs = &LittleFS;
  IdentityStore store(LittleFS, "/identity");
  store.begin();
#elif defined(ESP32)
  SPIFFS.begin(true);
  fs = &SPIFFS;
  IdentityStore store(SPIFFS, "/identity");
#else
  #error "need to define filesystem"
#endif
  if (!store.load("_main", the_mesh.self_id)) {
    the_mesh.self_id = radio_new_identity();   // create new random identity
    int count = 0;
    while (count < 10 && (the_mesh.self_id.pub_key[0] == 0x00 || the_mesh.self_id.pub_key[0] == 0xFF)) {  // reserved id hashes
      the_mesh.self_id = radio_new_identity(); count++;
    }
    store.save("_main", the_mesh.self_id);
  }

  Serial.print("Room ID: ");
  mesh::Utils::printHex(Serial, the_mesh.self_id.pub_key, PUB_KEY_SIZE); Serial.println();

  command[0] = 0;
#ifdef ETHERNET_ENABLED
  ethernet_command[0] = 0;
#endif

  sensors.begin();

  the_mesh.begin(fs);

#if defined(ETHERNET_ENABLED) && defined(WITH_MQTT_BRIDGE_ETHERNET)
  // ethernet_task (DHCP, on Core/priority 1 alongside this loop task) and the
  // bridge both touch the RAK13800 over the same SPI bus, which the vendor
  // library never locks. Hand the bridge the address of ethernet_running so
  // it never calls Ethernet.linkStatus()/localIP() until DHCP reports a
  // lease (C1).
  the_mesh.setEthernetReadyFlag(&ethernet_running);
#endif

#ifdef DISPLAY_CLASS
  if (display_ready) {
    ui_task.begin(the_mesh.getNodePrefs(), FIRMWARE_BUILD_DATE, FIRMWARE_VERSION);
  }
#endif

#ifdef ETHERNET_ENABLED
  ethernet_start_task();
#endif

  // send out initial zero hop Advertisement to the mesh
#if ENABLE_ADVERT_ON_BOOT == 1
  the_mesh.sendSelfAdvertisement(16000, false);
#endif

  board.onBootComplete();

#if defined(WITH_MQTT_BRIDGE_ETHERNET)
  startMainLoopTask();
#endif
}

static void mainLoopBody() {
  int len = strlen(command);
  while (Serial.available() && len < sizeof(command)-1) {
    char c = Serial.read();
    if (c != '\n') {
      command[len++] = c;
      command[len] = 0;
    }
    Serial.print(c);
  }
  if (len == sizeof(command)-1) {  // command buffer full
    command[sizeof(command)-1] = '\r';
  }

  if (len > 0 && command[len - 1] == '\r') {  // received complete line
    command[len - 1] = 0;  // replace newline with C string null terminator
    char reply[160];
    reply[0] = 0;
#ifdef ETHERNET_ENABLED
    if (!ethernet_handle_command(command, reply)) {
      the_mesh.handleCommand(0, command, reply);
    }
#else
    the_mesh.handleCommand(0, command, reply);  // NOTE: there is no sender_timestamp via serial!
#endif
    if (reply[0]) {
      Serial.print("  -> "); Serial.println(reply);
    }

    command[0] = 0;  // reset command buffer
  }

#ifdef ETHERNET_ENABLED
  ethernet_loop_maintain();
  if (ethernet_read_line(ethernet_command, sizeof(ethernet_command), the_mesh.getNodePrefs()->password)) {
    char reply[160];
    reply[0] = 0;
    if (!ethernet_handle_command(ethernet_command, reply)) {
      the_mesh.handleCommand(0, ethernet_command, reply);
    }
    ethernet_send_reply(reply);
    ethernet_command[0] = 0;
  }
#endif

  the_mesh.loop();
  sensors.loop();
#ifdef DISPLAY_CLASS
  if (display_ready) ui_task.loop();
#endif
  rtc_clock.tick();
#ifdef HAS_EXTERNAL_WATCHDOG
  external_watchdog.loop();
#endif
}

#if defined(WITH_MQTT_BRIDGE_ETHERNET)
// The Adafruit nRF52 core runs loop() on a fixed 4096-byte FreeRTOS stack
// (LOOP_STACK_SZ in the core's main.cpp, not overridable). With the Ethernet
// MQTT bridge on that task the high-water mark hit 8 bytes free before the
// first auth token was even built, and the board hard-reset about a second
// after each token publish. Re-host the whole main loop (radio, CLI, bridge)
// on one larger task so the single-task design and its SPI ownership stay
// intact, and park the core's loop task for good.
#ifndef MAIN_LOOP_STACK_BYTES
  #define MAIN_LOOP_STACK_BYTES 8192
#endif
static void mainLoopTask(void*) {
  for (;;) {
    mainLoopBody();
    yield();
  }
}
static void startMainLoopTask() {
  xTaskCreate(mainLoopTask, "mainloop", MAIN_LOOP_STACK_BYTES / sizeof(StackType_t), NULL, TASK_PRIO_LOW, NULL);
}
void loop() {
  suspendLoop();   // never returns; mainLoopTask owns the loop from here on
}
#else
void loop() {
  mainLoopBody();
}
#endif
