// Craig Hesling
// July 9, 2018
//
// This service serves as a compatibility layer for those user and services that
// expect data from on the /transducer/# subtopic prefix.
// This service bridges <dev_id>/+ space with <dev_id>/transducer/+ space
package main

import (
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/openchirp/framework"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	version string = "1.0"
)

const (
	// Set this value to true to have the service publish a service status of
	// "Running" each time it receives a device update event
	runningStatus = true
)

const (
	TransducerPrefix = "transducer"
)

const (
	keyTopicTransducer int = iota
	keyTopicBase
)

// Device holds any data you want to keep around for a specific
// device that has linked your service.
//
// In this example, we will keep track of the rawrx and rawtx message counts
type Device struct {
}

// NewDevice is called by the framework when a new device has been linked.
func NewDevice() framework.Device {
	d := new(Device)
	return d
}

// ProcessLink is called once, during the initial setup of a
// device, and is provided the service config for the linking device.
func (d *Device) ProcessLink(ctrl *framework.DeviceControl) string {
	logitem := log.WithField("OCID", ctrl.Id())
	logitem.Debug("Linking with config:", ctrl.Config())

	// Subscribe to "+" topics
	ctrl.Subscribe("+", keyTopicBase)

	// Subscribe to "transducer/+" topics
	ctrl.Subscribe(TransducerPrefix+"/+", keyTopicTransducer)

	logitem.Debug("Finished Linking")

	return "Success"
}

// ProcessUnlink is called once, when the service has been unlinked from
// the device.
func (d *Device) ProcessUnlink(ctrl *framework.DeviceControl) {
	logitem := log.WithField("OCID", ctrl.Id())
	logitem.Debug("Unlinked:")
}

// ProcessConfigChange is intended to handle a service config updates.
func (d *Device) ProcessConfigChange(ctrl *framework.DeviceControl, cchanges, coriginal map[string]string) (string, bool) {
	logitem := log.WithField("OCID", ctrl.Id())

	logitem.Debug("Ignoring Config Change:", cchanges)
	return "", false
}

// ProcessMessage is called upon receiving a pubsub message destined for
// this device.
func (d *Device) ProcessMessage(ctrl *framework.DeviceControl, msg framework.Message) {
	logitem := log.WithField("OCID", ctrl.Id())
	logitem.Debugf("Processing Message: %v: [ % #x ]", msg.Key(), msg.Payload())

	if msg.Key().(int) == keyTopicBase {
		subtopic := TransducerPrefix + "/" + msg.Topic()
		ctrl.Publish(subtopic, msg.Payload())
	} else if msg.Key().(int) == keyTopicTransducer {
		subtopic := strings.TrimPrefix(msg.Topic(), TransducerPrefix+"/")
		ctrl.Publish(subtopic, msg.Payload())
	} else {
		logitem.Errorln("Received unassociated message")
	}
}

// run is the main function that gets called once form main()
func run(ctx *cli.Context) error {
	/* Set logging level (verbosity) */
	log.SetLevel(log.Level(uint32(ctx.Int("log-level"))))

	log.Info("Starting Legacy Transducer Prefix Service")

	/* Start framework service client */
	framework.MQTTBridgeClient = true
	c, err := framework.StartServiceClientManaged(
		ctx.String("framework-server"),
		ctx.String("mqtt-server"),
		ctx.String("service-id"),
		ctx.String("service-token"),
		"Unexpected disconnect!",
		NewDevice)
	if err != nil {
		log.Error("Failed to StartServiceClient: ", err)
		return cli.NewExitError(nil, 1)
	}
	defer c.StopClient()
	log.Info("Started service")

	/* Post service's global status */
	if err := c.SetStatus("Starting"); err != nil {
		log.Error("Failed to publish service status: ", err)
		return cli.NewExitError(nil, 1)
	}
	log.Info("Published Service Status")

	/* Setup signal channel */
	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	/* Post service status indicating I started */
	if err := c.SetStatus("Started"); err != nil {
		log.Error("Failed to publish service status: ", err)
		return cli.NewExitError(nil, 1)
	}
	log.Info("Published Service Status")

	/* Wait on a signal */
	sig := <-signals
	log.Info("Received signal ", sig)
	log.Warning("Shutting down")

	/* Post service's global status */
	if err := c.SetStatus("Shutting down"); err != nil {
		log.Error("Failed to publish service status: ", err)
	}
	log.Info("Published service status")

	return nil
}

func main() {
	/* Parse arguments and environmental variable */
	app := cli.NewApp()
	app.Name = "transducer-prefix-service"
	app.Usage = ""
	app.Copyright = "See https://github.com/openchirp/transducer-prefix-service for copyright information"
	app.Version = version
	app.Action = run
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "framework-server",
			Usage:  "OpenChirp framework server's URI",
			Value:  "http://localhost:7000",
			EnvVar: "FRAMEWORK_SERVER",
		},
		cli.StringFlag{
			Name:   "mqtt-server",
			Usage:  "MQTT server's URI (e.g. scheme://host:port where scheme is tcp or tls)",
			Value:  "tls://localhost:8883",
			EnvVar: "MQTT_SERVER",
		},
		cli.StringFlag{
			Name:   "service-id",
			Usage:  "OpenChirp service id",
			EnvVar: "SERVICE_ID",
		},
		cli.StringFlag{
			Name:   "service-token",
			Usage:  "OpenChirp service token",
			EnvVar: "SERVICE_TOKEN",
		},
		cli.IntFlag{
			Name:   "log-level",
			Value:  4,
			Usage:  "debug=5, info=4, warning=3, error=2, fatal=1, panic=0",
			EnvVar: "LOG_LEVEL",
		},
	}

	/* Launch the application */
	app.Run(os.Args)
}
