# Copyright (c) 2016 Yingxin Cheng
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import inspect
import traceback


class AopPatch(object):
    error_prefix = "AOP_PATCH_ERROR"
    logger = None
    printer = None
    exc_key = "exc_val"
    ret_key = "ret_val"

    def _handle_error(self, e, place, arg_dict):
        e_stack = traceback.format_exc()
        self.logger(self.error_prefix +
                    (" %s@%s " % (self.name, place)) + str(e))
        self.logger(self.error_prefix + " " + str(arg_dict.keys()))
        self.logger(self.error_prefix + " " + repr(arg_dict))
        self.logger(self.error_prefix + " " + e_stack)

    def aop_wrapper(self, func):
        # Getting real signature of the decorated function
        # This looks ugly becouse some of the decorators don't preserve the
        # original signature, which is bad for variable name introspection
        # TODO: Need to find a better way to introspect variable names
        inspected = func
        while True:
            # NOTE: python 3 uses __closure__ instead
            inspected_closure = inspected.func_closure
            if inspected_closure is None:
                break
            found = False
            for closure in inspected_closure:
                c_content = closure.cell_contents
                if inspect.isfunction(c_content):
                    inspected = c_content
                    found = True
                    break
            if not found:
                break
        func_varnames = inspected.func_code.co_varnames

        def aop_wrapped(*args, **kwargs):
            # NOTE(Yingxin): It doesn't handle default arguments.
            arg_dict = {}
            arg_dict.update(kwargs)

            for key, item in zip(func_varnames, args):
                arg_dict[key] = item

            if self.before:
                try:
                    if self.direct:
                        self.before(arg_dict)
                    else:
                        self.printer(self.before(arg_dict))
                except Exception as e:
                    self._handle_error(e, "BEFORE", arg_dict)
            try:
                ret = func(*args, **kwargs)
            except Exception as e:
                if self.excep:
                    if self.exc_key in arg_dict:
                        self.logger(self.error_prefix + " %s key confliction!"
                                    % self.exc_key)
                    else:
                        arg_dict[self.exc_key] = e
                    try:
                        if self.direct:
                            self.excep(arg_dict)
                        else:
                            self.printer(self.excep(arg_dict))
                    except Exception as e:
                        self._handle_error(e, "EXCEPT", arg_dict)
                raise
            if self.after:
                if self.ret_key in arg_dict:
                    self.logger(self.error_prefix + " %s key confliction!"
                                % self.ret_key)
                else:
                    arg_dict[self.ret_key] = ret
                try:
                    if self.direct:
                        self.after(arg_dict)
                    else:
                        self.printer(self.after(arg_dict))
                except Exception as e:
                    self._handle_error(e, "AFTER", arg_dict)
            return ret
        return aop_wrapped

    def __init__(self, name,
                 before=None, after=None, excep=None, direct=False):
        self.name = name
        self.before = before
        self.after = after
        self.excep = excep
        self.direct = direct
        self.patching = MonkeyPatch(name, self.aop_wrapper, wrap=True)

    def clean(self):
        self.patching.clean()


class MonkeyPatch(object):
    def __init__(self, name, attr, wrap=False, add=False):
        location, attribute = name.rsplit('.', 1)
        try:
            __import__(location, {}, {})
        except ImportError:
            pass
        components = location.split('.')
        current = __import__(components[0], {}, {})
        for component in components[1:]:
            current = getattr(current, component)
        sentinel = object()
        old_value = getattr(current, attribute, sentinel)
        if not add:
            if old_value is sentinel:
                err_msg = "Cannot find method: %s" % name
                AopPatch.logger(AopPatch.error_prefix + " " + err_msg)
                raise RuntimeError(err_msg)
        else:
            if old_value is not sentinel:
                err_msg = "Method already exists: %s" % name
                AopPatch.logger(AopPatch.error_prefix + " " + err_msg)
                raise RuntimeError(err_msg)

        self.attribute = attribute
        self.old_value = old_value
        self.current = current
        self.name = name
        self.add = add

        if wrap:
            setattr(current, attribute, attr(old_value))
        else:
            setattr(current, attribute, attr)

    def clean(self):
        if not self.add:
            setattr(self.current, self.attribute, self.old_value)
        else:
            delattr(self.current, self.attribute)
