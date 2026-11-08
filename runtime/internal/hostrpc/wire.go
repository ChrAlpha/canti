package hostrpc

import wire "github.com/ChrAlpha/canti/sdk/go/hostrpc"

type Request = wire.Request
type Response = wire.Response
type Error = wire.Error
type Hello = wire.Hello
type Pair = wire.Pair
type Spec = wire.Spec
type Submission = wire.Submission
type Binding = wire.Binding
type Resolution = wire.Resolution
type Ack = wire.Ack
type Run = wire.Run
type Totals = wire.Totals
type Budget = wire.Budget
type Page = wire.Page
type Item = wire.Item
type Outcome = wire.Outcome
type Wait = wire.Wait
type Inspection = wire.Inspection
type Receipt = wire.Receipt

const OpHello = wire.OpHello
const OpSubmit = wire.OpSubmit
const OpDrive = wire.OpDrive
const OpInspect = wire.OpInspect
const OpView = wire.OpView
const OpProgress = wire.OpProgress
const OpWait = wire.OpWait
const OpStop = wire.OpStop
const OpResolve = wire.OpResolve
const OpInherited = wire.OpInherited
const OpShutdown = wire.OpShutdown
const CodeProtocol = wire.CodeProtocol
const CodeSpec = wire.CodeSpec
const CodeConflict = wire.CodeConflict
const CodeClosing = wire.CodeClosing
const CodeDriving = wire.CodeDriving
const CodeCursor = wire.CodeCursor
const CodeFailed = wire.CodeFailed
const StatusActive = wire.StatusActive
const StatusNeedsResolution = wire.StatusNeedsResolution
const StatusReturned = wire.StatusReturned
const StatusStopped = wire.StatusStopped
const DriverDriving = wire.DriverDriving
const DriverIdle = wire.DriverIdle
const Protocol = wire.Protocol

type Validation = wire.Validation
type Validated = wire.Validated

const OpValidate = wire.OpValidate
