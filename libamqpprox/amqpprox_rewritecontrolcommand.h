// SPDX-License-Identifier: Apache-2.0
#ifndef BLOOMBERG_AMQPPROX_REWRITECONTROLCOMMAND
#define BLOOMBERG_AMQPPROX_REWRITECONTROLCOMMAND

#include <amqpprox_controlcommand.h>

#include <string>

namespace Bloomberg {
namespace amqpprox {

/**
 * \brief Control command to inspect the vhost rewrite configuration and
 * reload the user to vhost map file at runtime, implements the
 * ControlCommand interface
 */
class RewriteControlCommand : public ControlCommand {
    std::string d_userVhostMapFile;

  public:
    /**
     * \param userVhostMapFile the --rewriteVhostUserMap file, empty if
     * per-user mapping is not enabled
     */
    explicit RewriteControlCommand(std::string userVhostMapFile);

    /**
     * \return the command verb this handles
     */
    virtual std::string commandVerb() const override;

    /**
     * \return a string of the help text for this command
     */
    virtual std::string helpText() const override;

    /**
     * \brief Execute a command, providing any output to the provided functor
     * \param command to execute
     * \param restOfCommand parameters for the command
     * \param outputFunctor is called back with the output
     * \param serverHandle access to the Server object
     * \param controlHandle access to the Control object
     */
    virtual void handleCommand(const std::string   &command,
                               const std::string   &restOfCommand,
                               const OutputFunctor &outputFunctor,
                               Server              *serverHandle,
                               Control             *controlHandle) override;
};

}
}

#endif
