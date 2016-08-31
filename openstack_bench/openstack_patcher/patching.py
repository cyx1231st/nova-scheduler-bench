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


class AopPatch(object):
    logger = None

    def aop_wrapper(self, func):
        def aop_wrapped(*args, **kwargs):
            if self.before:
                try:
                    self.before(*args, **kwargs)
                except Exception as e:
                    self.logger(str(e))
                    self.logger("args: %s" % repr(args))
                    self.logger("kwargs: %s" % repr(kwargs))
                    raise
            try:
                ret = func(*args, **kwargs)
            except Exception as e:
                if self.after:
                    try:
                        self.after(ret_val=None, exc_val=e, *args, **kwargs)
                    except Exception as e:
                        self.logger(str(e))
                        self.logger("args: %s" % repr(args))
                        self.logger("kwargs: %s" % repr(kwargs))
                        raise
                raise
            if self.after:
                try:
                    self.after(ret_val=ret, exc_val=None, *args, **kwargs)
                except Exception as e:
                    self.logger(str(e))
                    self.logger("args: %s" % repr(args))
                    self.logger("kwargs: %s" % repr(kwargs))
                    raise
            return ret
        return aop_wrapped

    def __init__(self, name, before=None, after=None):
        self.name = name
        self.before = before
        self.after = after
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
                raise RuntimeError("Cannot find method: %s" % name)
        else:
            if old_value is not sentinel:
                raise RuntimeError("Method already exists: %s" % name)

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
