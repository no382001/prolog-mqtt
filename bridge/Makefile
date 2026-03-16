PAHO_DIR =../ paho.mqtt.c PAHO_INC =
              $(PAHO_DIR) / src PAHO_BUILD =
                  $(PAHO_DIR) / build PAHO_LIB =
                      $(PAHO_BUILD) / src / libpaho - mqtt3as.so

                                                          CC = gcc CFLAGS =
                          -O2 - Wall - std =
                              gnu99 - I$(PAHO_INC) LDFLAGS =
                                  -L$(PAHO_BUILD) / src - lpaho - mqtt3as -
                                  lpthread - lssl - lcrypto - Wl,
                          -rpath, / usr / local /
                                      lib

                                          TARGET = mqtt_bridge

                                                       .PHONY : all paho clean

                                                                    all
    : paho
      $(TARGET)

          paho : $(PAHO_LIB)

                     $(PAHO_LIB)
    : cmake - S $(PAHO_DIR) - B $(PAHO_BUILD) - DPAHO_BUILD_STATIC =
                                                       OFF - DPAHO_WITH_SSL =
                                                           ON -
                                                           DPAHO_BUILD_SAMPLES =
                                                               OFF -
                                                               DPAHO_BUILD_DOCUMENTATION =
                                                                   OFF cmake-- build $(
                                                                       PAHO_BUILD)

                                                                           $(TARGET)
    : mqtt_bridge.c $(PAHO_LIB) $(CC) $(CFLAGS) - o $ @$ < $(LDFLAGS)

                                                                           clean
    : rm -
      f $(TARGET)
